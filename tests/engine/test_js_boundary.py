"""JS 规则在 phase 1 必须**显式失败**，以及遮蔽顺序的正确性。

两件事在这里钉住：

1. `NullJsRuntime` 一律抛 `JsNotSupportedError`，绝不静默返回空或降级 —— 实测
   48% 的单点 JS 阻塞落在 URL 生成型字段上，丢掉 JS 只会拿到裸 id、发出错误请求。
2. `mask_js` 必须在任何 split 之前跑完。JS 源码里的 `&&` / `||` / `##` 一旦被
   当成组合符或净化分隔符，规则就会被切碎成语法垃圾。
"""

import pytest

from funread.legado.engine import RuleEvaluator
from funread.legado.engine.errors import JsNotSupportedError
from funread.legado.engine.js import JsContext, NullJsRuntime
from funread.legado.engine.lexer import parse_rule, scan_features
from funread.legado.engine.nodes import DefaultAtom, JsAtom

HTML = '<html><body><div id="x">文本</div><a id="u" href="/c/1">章</a></body></html>'


@pytest.fixture
def ev():
    return RuleEvaluator(HTML, base_url="https://example.com/")


# ------------------------------------------------------- 显式失败，不降级


@pytest.mark.parametrize(
    "rule",
    [
        '@js:result+"x"',
        "<js>java.ajax(baseUrl)</js>",
        'id.x@text@js:result.replace("文","字")',
        '$.url@js:"https://x"+result',
        '{{java.ajax("https://x")}}',
    ],
)
def test_js_rules_raise(ev, rule):
    with pytest.raises(JsNotSupportedError):
        ev.string(rule)


def test_js_in_leading_position_also_raises(ev):
    """58% 的 JS 是 leading 形态（`@js:` 独占整个字段），没有可保留的选择器部分。"""
    with pytest.raises(JsNotSupportedError):
        ev.rows("<js>java.getElements('#list li')</js>")


def test_js_error_carries_script_snippet(ev):
    with pytest.raises(JsNotSupportedError) as excinfo:
        ev.string('@js:java.ajax("https://example.com/api")')
    assert "java.ajax" in str(excinfo.value)


def test_js_rule_does_not_fall_back_to_selector_part(ev):
    """`id.x@text@js:...` 不能退化成 `id.x@text` 的结果 —— 明确放弃降级。"""
    with pytest.raises(JsNotSupportedError):
        ev.string("id.x@text@js:result")


def test_null_runtime_raises_directly():
    with pytest.raises(JsNotSupportedError):
        NullJsRuntime().eval("1+1", JsContext())


def test_long_script_snippet_is_truncated():
    with pytest.raises(JsNotSupportedError) as excinfo:
        NullJsRuntime().eval("x" * 500, JsContext())
    assert "..." in str(excinfo.value)


# ---------------------------------------------------------- 遮蔽顺序正确性


def _atoms(rule):
    program = parse_rule(rule)
    return [atom for group in program.groups for or_item in group for atom in or_item]


def test_js_body_with_combinators_is_not_split():
    """`&&` / `||` 在 JS 源码里，不能被当成组合符。"""
    rule = "@js:if(a&&b||c){1}"
    program = parse_rule(rule)
    atoms = _atoms(rule)
    assert len(program.groups) == 1
    assert len(atoms) == 1
    assert isinstance(atoms[0], JsAtom)
    assert atoms[0].script == "if(a&&b||c){1}"


def test_js_body_with_hashes_is_not_purified():
    """`##` 在 JS 源码里，不能被当成净化分隔符。"""
    rule = "@js:if(a&&b){x##y}"
    program = parse_rule(rule)
    assert program.purify is None
    atoms = _atoms(rule)
    assert isinstance(atoms[0], JsAtom)
    assert atoms[0].script == "if(a&&b){x##y}"


def test_tag_js_body_with_combinators_is_not_split():
    rule = "<js>var s=a&&b;s%%2</js>"
    atoms = _atoms(rule)
    assert len(atoms) == 1
    assert isinstance(atoms[0], JsAtom)
    assert atoms[0].form == "tag_js"
    assert atoms[0].script == "var s=a&&b;s%%2"


def test_selector_then_js_keeps_both_nodes():
    """`@js:` 是管道里的一环，解析结果里 JS 必须和方言节点同级存在。"""
    atoms = _atoms("id.x@text@js:result")
    assert len(atoms) == 1  # 同一个 `&&` 片段，只是尾部被遮蔽
    program = parse_rule("id.x@text&&@js:result")
    flat = [a for g in program.groups for oi in g for a in oi]
    assert len(program.groups) == 2
    assert isinstance(flat[0], DefaultAtom)
    assert isinstance(flat[1], JsAtom)


def test_purify_outside_js_still_works_alongside_js():
    program = parse_rule("id.x@text##文##字")
    assert program.purify is not None
    assert program.purify.pattern.pattern == "文"


def test_url_with_hash_fragment_is_not_purified():
    """`https://x/#/a` 里的 `#` 不是净化分隔符。"""
    program = parse_rule("https://example.com/#/book/1")
    assert program.purify is None


# ------------------------------------------------------ 静态扫描（选源用）


@pytest.mark.parametrize(
    "rules,needs_js",
    [
        (["id.x@text", "$.name", "@css:a@href"], False),
        (["/search?q={{key}}&p={{page}}"], False),
        (["/s?p={{(page-1)*20}}"], False),
        (["id.x@text@js:result"], True),
        (["<js>1</js>"], True),
        (['{{java.ajax("x")}}'], True),
        (["{{book.name}}"], True),
        ([None, "", "id.x@text"], False),
    ],
)
def test_scan_features_detects_js_without_network(rules, needs_js):
    assert scan_features(rules)["needs_js"] is needs_js
