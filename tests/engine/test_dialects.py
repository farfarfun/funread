"""`@css:` / `@xpath:` / `//` / `@json:` / `$.` 五种方言前缀。"""

import pytest

from funread.legado.engine import RuleEvaluator
from funread.legado.engine.errors import JsonPathNotSupportedError

HTML = """
<html><body>
  <ul class="list">
    <li class="item"><a href="/b/1" title="书一">书一</a></li>
    <li class="item"><a href="/b/2" title="书二">书二</a></li>
  </ul>
  <div class="meta"><span>作者</span><span>甲</span></div>
</body></html>
"""

JSON = """
{"data": {"total": 2, "books": [
  {"id": 1, "name": "书一", "author": "甲", "hot": true},
  {"id": 2, "name": "书二", "author": "乙", "hot": false}
]}}
"""


@pytest.fixture
def html_ev():
    return RuleEvaluator(HTML, base_url="https://example.com/s/")


@pytest.fixture
def json_ev():
    return RuleEvaluator(JSON, base_url="https://example.com/api/")


# ----------------------------------------------------------------- CSS


def test_css_prefix_select_and_text(html_ev):
    assert html_ev.strings("@css:ul.list li.item a@text") == ["书一", "书二"]


def test_css_prefix_attr(html_ev):
    assert html_ev.strings("@css:li.item > a@title") == ["书一", "书二"]


def test_css_href_absolutized(html_ev):
    assert html_ev.urls("@css:li.item a@href") == [
        "https://example.com/b/1",
        "https://example.com/b/2",
    ]


def test_css_rows_then_relative_rule(html_ev):
    rows = html_ev.rows("@css:li.item")
    assert len(rows) == 2
    assert [row.string("tag.a@text") for row in rows] == ["书一", "书二"]


# --------------------------------------------------------------- XPath


def test_xpath_prefix(html_ev):
    assert html_ev.strings('@xpath://li[@class="item"]/a/text()') == ["书一", "书二"]


def test_bare_double_slash_is_xpath(html_ev):
    assert html_ev.strings("//li/a/@href") == ["/b/1", "/b/2"]


def test_xpath_element_result_can_be_rows(html_ev):
    rows = html_ev.rows("//ul[@class='list']/li")
    assert [row.string("tag.a@title") for row in rows] == ["书一", "书二"]


def test_css_bad_selector_raises_syntax_error(html_ev):
    """`@css:` 是作者明写的选择器，翻译不了就是规则写错 —— 必须报错。

    对照 `test_untranslatable_guessed_css_is_a_miss`：Default 方言「猜」出来的
    CSS 翻不动算没命中，这两条路的严格程度是刻意不同的。
    """
    from funread.legado.engine.errors import RuleSyntaxError

    with pytest.raises(RuleSyntaxError):
        html_ev.strings("@css:li:eq(0)")


def test_xpath_bad_expression_raises_syntax_error(html_ev):
    from funread.legado.engine.errors import RuleSyntaxError

    with pytest.raises(RuleSyntaxError):
        html_ev.strings("@xpath://li[")


# ------------------------------------------------------------- JSONPath


def test_json_prefix_list_expands_to_rows(json_ev):
    rows = json_ev.rows("$.data.books[*]")
    assert len(rows) == 2
    assert [row.string("$.name") for row in rows] == ["书一", "书二"]


def test_json_at_prefix_equivalent(json_ev):
    assert json_ev.strings("@json:$.data.books[*].author") == ["甲", "乙"]


def test_json_numeric_and_bool_stringified(json_ev):
    assert json_ev.string("$.data.total") == "2"
    assert json_ev.string("$.data.books[0].hot") == "true"
    assert json_ev.string("$.data.books[1].hot") == "false"


def test_json_ext_filter_supported(json_ev):
    """用的是 jsonpath_ng.ext，过滤表达式必须能跑。"""
    assert json_ev.strings('$.data.books[?(@.author=="乙")].name') == ["书二"]


def test_json_missing_path_is_empty_not_error(json_ev):
    assert json_ev.string("$.data.nope") == ""
    assert json_ev.strings("$.data.nope[*]") == []


def test_unsupported_jsonpath_raises_typed_error(json_ev):
    with pytest.raises(JsonPathNotSupportedError):
        json_ev.strings("$.data.books[?(")


# --------------------------------------------------- 跨 backend 衔接


def test_html_fragment_inside_json_field():
    """JSON 字段里装 HTML 片段 —— 取出后要能继续用 Default 方言定位。"""
    ev = RuleEvaluator('{"content": "<p>第一段</p><p>第二段</p>"}')
    rows = ev.rows("$.content")
    assert len(rows) == 1
    assert rows[0].strings("tag.p@text") == ["第一段", "第二段"]


def test_json_rows_keep_json_backend_for_default_attr():
    """JSON 行上写裸属性名（不带 `$.`）也要能取到字段。"""
    ev = RuleEvaluator(JSON)
    rows = ev.rows("$.data.books[*]")
    assert [row.string("@name") for row in rows] == ["书一", "书二"]


def test_auto_content_type_detects_json():
    assert RuleEvaluator('  {"a": 1}').string("$.a") == "1"
    assert RuleEvaluator("  <div>x</div>").string("tag.div@text") == "x"


def test_string_scope_promotes_to_html_for_selectors():
    """字符串作用域上写定位规则 → 自动按 HTML 解析一遍（跨 backend 衔接）。

    不报错是故意的：JSON 字段里装 HTML 片段这个场景太常见，必须让定位规则直接能用。
    纯文本配不上任何选择器时返回空，这就是「规则没命中」的正常结果。
    """
    ev = RuleEvaluator("纯文本", content_type="text")
    assert ev.text == "纯文本"
    assert ev.string("tag.div@text") == ""

    html_in_string = RuleEvaluator("<div>命中</div>", content_type="text")
    assert html_in_string.string("tag.div@text") == "命中"
