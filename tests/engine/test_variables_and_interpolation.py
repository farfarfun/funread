"""`@put` / `@get` 变量池、`{{}}` 插值、纯算术占位符。"""

import pytest

from funread.legado.engine import RuleEvaluator
from funread.legado.engine.errors import JsNotSupportedError
from funread.legado.engine.expr import MiniExpr
from funread.legado.engine.variables import VariableScope

HTML = """
<html><body>
  <div id="meta" data-bid="7788" data-token="abc">元信息</div>
  <div id="name">剑来</div>
  <ul id="list">
    <li data-cid="1"><a href="/c/1">第一章</a></li>
    <li data-cid="2"><a href="/c/2">第二章</a></li>
  </ul>
</body></html>
"""


@pytest.fixture
def ev():
    return RuleEvaluator(HTML, base_url="https://example.com/")


# ------------------------------------------------------------ @put / @get


def test_put_captures_into_scope(ev):
    ev.string("id.meta@put:{bid:id.meta@data-bid}@text")
    assert ev.variables.get("bid") == "7788"


def test_get_substitutes_into_url_template(ev):
    """`@get:` 的主战场是 URL 模板，走 `template()` 而不是选择器求值。"""
    ev.string("@put:{bid:id.meta@data-bid}")
    assert ev.variables.get("bid") == "7788"
    assert ev.template("/book/@get:{bid}.html") == "/book/7788.html"
    assert ev.template("@get:{bid}") == "7788"


def test_get_of_unset_key_is_empty_string(ev):
    assert ev.template("@get:{nope}") == ""
    assert ev.template("/b/@get:{nope}/") == "/b//"


def test_put_only_rule_returns_empty_but_still_captures(ev):
    """整条规则只有 `@put` 时结果是空的，但变量必须落进池子。"""
    assert ev.string("@put:{token:id.meta@data-token}") == ""
    assert ev.variables.get("token") == "abc"


def test_get_bypasses_parse_cache(ev):
    """含 `@get:` 的规则串不能走 lru_cache —— 同一串两次求值结果必须跟着变量变。"""
    ev.variables.put("v", "first")
    assert ev.template("@get:{v}") == "first"
    ev.variables.put("v", "second")
    assert ev.template("@get:{v}") == "second"


def test_get_inside_selector_rule_bypasses_parse_cache(ev):
    """选择器规则里的 `@get:`（这里当索引用）同样不能被缓存住。"""
    ev.variables.put("i", "0")
    assert ev.string("id.list@tag.a.@get:{i}@text") == "第一章"
    ev.variables.put("i", "1")
    assert ev.string("id.list@tag.a.@get:{i}@text") == "第二章"


def test_row_scopes_are_isolated_but_inherit(ev):
    ev.variables.put("shared", "页级")
    rows = ev.rows("id.list@tag.li")
    assert len(rows) == 2
    for index, row in enumerate(rows):
        row.string("@put:{cid:@data-cid}")
        assert row.variables.get("shared") == "页级"
    # 行级 put 不污染页级，也不互相污染
    assert ev.variables.get("cid") == ""
    assert [row.variables.get("cid") for row in rows] == ["1", "2"]


def test_scope_snapshot_includes_inherited():
    parent = VariableScope(data={"a": "1"})
    child = parent.child({"b": "2"})
    assert child.snapshot() == {"a": "1", "b": "2"}
    assert child.local() == {"b": "2"}
    assert "a" in child and "a" not in VariableScope()


# --------------------------------------------------------------- {{}} 插值


def test_key_placeholder():
    ev = RuleEvaluator("", context={"key": "剑来"})
    assert ev.string("{{key}}") == "剑来"


def test_page_placeholder():
    ev = RuleEvaluator("", context={"page": 3})
    assert ev.string("{{page}}") == "3"


def test_arithmetic_placeholder():
    ev = RuleEvaluator("", context={"page": 3})
    assert ev.string("{{(page-1)*20}}") == "40"
    assert ev.string("{{page+1}}") == "4"


def test_interpolation_mixed_with_literal():
    """字面量 + 占位符混排：字面量部分必须原样留着。"""
    ev = RuleEvaluator("", context={"key": "剑来", "page": 2})
    assert ev.template("/search?q={{key}}&p={{page}}") == "/search?q=剑来&p=2"
    # 选择器入口上同样按片展开，不会把整串丢给 JS runtime
    assert ev.string("/search?q={{key}}&p={{page}}") == "/search?q=剑来&p=2"


def test_template_without_placeholder_is_passthrough():
    ev = RuleEvaluator("", context={"key": "x"})
    assert ev.template("/search?q=abc") == "/search?q=abc"
    assert ev.template("") == ""
    assert ev.template(None) == ""


def test_non_placeholder_interpolation_needs_js():
    """`{{}}` 里是真 JS 时必须显式失败，不能静默返回空串。"""
    ev = RuleEvaluator("", context={"key": "x"})
    with pytest.raises(JsNotSupportedError):
        ev.string('{{java.ajax("https://x")}}')
    with pytest.raises(JsNotSupportedError):
        ev.template("/s?t={{java.timeFormat()}}")


# ----------------------------------------------------------------- MiniExpr


@pytest.mark.parametrize(
    "expr,expected",
    [
        ("page", 2),
        ("page+1", 3),
        ("(page-1)*20", 20),
        ("page*10-5", 15),
        ("20*(page-1)+1", 21),
        ("-page", -2),
        ("page//2", 1),
        ("page%2", 0),
        ("2**page", 4),
    ],
)
def test_mini_expr_accepts_arithmetic(expr, expected):
    assert MiniExpr.try_eval(expr, {"page": 2, "key": ""}) == expected


@pytest.mark.parametrize(
    "expr",
    [
        'java.ajax("x")',
        "key.length",
        "__import__('os').system('ls')",
        "open('/etc/passwd')",
        "[1,2,3]",
        "page if page else 1",
        "lambda: 1",
        "page.__class__",
        "unknown_name+1",
    ],
)
def test_mini_expr_refuses_anything_else(expr):
    """求不了就返回 None，由调用方交给 JS runtime —— 绝不 eval 任意代码。"""
    assert MiniExpr.try_eval(expr, {"page": 2, "key": ""}) is None
