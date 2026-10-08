"""Default(JSOUP) 方言：定位与取值。"""

import pytest

from funread.legado.engine import RuleEvaluator

HTML = """
<html><body>
  <div class="a foo b" id="hit">命中</div>
  <div class="foobar">不该命中</div>
  <div class="size16 color5 pt-read-text">多 token</div>
  <ul id="list">
    <li><a href="/book/1">书一</a></li>
    <li><a href="/book/2">书二</a></li>
    <li><a href="/book/3">书三</a></li>
  </ul>
  <div id="own">
    直接文字
    <span>子元素文字</span>
    尾巴文字
  </div>
  <div id="nodes">第一行<br>第二行<br>   <br>第三行</div>
  <p id="intro">简介：<em>很好看</em>的一本书</p>
  <meta property="og:description" content="来自 meta">
  <div id="toc-wrap"><a href="/toc/9">目录</a><a href="/other">其它</a></div>
  <img id="pic" src="//cdn.example.com/p.jpg">
</body></html>
"""


@pytest.fixture
def ev():
    return RuleEvaluator(HTML, base_url="https://example.com/book/")


def test_class_token_match_not_substring(ev):
    assert ev.string("class.foo@text") == "命中"


def test_class_multi_token(ev):
    assert ev.string("class.size16 color5 pt-read-text@text") == "多 token"


def test_id_and_tag(ev):
    assert ev.string("id.hit@text") == "命中"
    assert ev.strings("id.list@tag.a@text") == ["书一", "书二", "书三"]


def test_select_includes_the_root_itself(ev):
    """对齐 jsoup：`Element.select()` 从根节点自身开始遍历，根匹配就算命中。

    行规则把作用域收到 `.bookinfo` 上之后，字段规则还会再写一遍
    `class.bookinfo@tag.a@href` —— 真实源里非常常见（实测 vipyzw）。用 `.//`
    的话根自己被排除，这类字段全取空。
    """
    row = ev.rows("class.foo")[0]
    assert row.string("class.foo@text") == "命中"
    assert row.string("tag.div@text") == "命中"
    assert ev.rows("id.list")[0].string("id.list@tag.a.0@text") == "书一"


def test_index_single_and_negative(ev):
    assert ev.string("id.list@tag.a.0@text") == "书一"
    assert ev.string("id.list@tag.a.-1@text") == "书三"


def test_index_exclude(ev):
    assert ev.strings("id.list@tag.a.!0@text") == ["书二", "书三"]


def test_index_exclude_without_leading_dot(ev):
    """回归：`!` 排除**不带前导点**也是合法形态。

    Legado 先按 `!` 切掉排除项再做类型派发，所以 `a!0` = 选择器 `a` 去掉第 0 个。
    以前整个 `a!0` 会原样丢给 cssselect，抛 `RuleSyntaxError` 把整个源判死
    （实测 EHentai 源的 `tr!0` 就是这么挂的）。
    """
    assert ev.strings("id.list@a!0@text") == ["书二", "书三"]
    assert ev.strings("id.list@tag.a!0,1@text") == ["书三"]


def test_untranslatable_guessed_css_is_a_miss(ev):
    """猜出来的 CSS 翻不动就是「没命中」，不是报错。

    jsoup 有 `:eq()`/`:containsOwn()` 这类 cssselect 不认的伪类，为它们抛错会
    把整个源判死 —— 而 `@css:` 是作者明确写的选择器，那条路照旧报错。
    """
    assert ev.strings("id.list@a:eq(0)@text") == []
    #  `tag.` 分支同理：名字拼进 XPath 前先验，不合法就算没命中
    assert ev.strings("id.list@tag.a:eq(0)@text") == []


def test_index_list_and_slice(ev):
    assert ev.strings("id.list@tag.a[0,2]@text") == ["书一", "书三"]
    assert ev.strings("id.list@tag.a[0:2]@text") == ["书一", "书二"]
    assert ev.strings("id.list@tag.a[::2]@text") == ["书一", "书三"]


def test_children_and_bare_number(ev):
    assert ev.string("id.list@children@tag.a.0@text") == "书一"
    assert ev.string("id.list@1@tag.a@text") == "书二"


def test_text_keyword_uses_own_text(ev):
    """text.目录 必须命中 <a>，不能命中它的祖先 div/body。"""
    assert ev.url("text.目录@href") == "https://example.com/toc/9"


def test_own_text_ignores_child_elements(ev):
    own = ev.string("id.own@ownText")
    assert "子元素文字" not in own
    assert "直接文字" in own and "尾巴文字" in own


def test_text_nodes_splits_lines(ev):
    nodes = ev.string("id.nodes@textNodes")
    assert nodes.splitlines() == ["第一行", "第二行", "第三行"]


def test_text_includes_descendants(ev):
    assert ev.string("id.intro@text") == "简介：很好看的一本书"


def test_html_is_inner_and_all_is_outer(ev):
    assert ev.string("id.intro@html").startswith("简介：<em>")
    assert ev.string("id.intro@all").startswith('<p id="intro">')


def test_href_absolutized(ev):
    assert ev.strings("id.list@tag.a@href") == [
        "https://example.com/book/1",
        "https://example.com/book/2",
        "https://example.com/book/3",
    ]


def test_protocol_relative_src(ev):
    assert ev.url("id.pic@src") == "https://cdn.example.com/p.jpg"


def test_bare_css_fallback(ev):
    assert ev.string('[property="og:description"]@content') == "来自 meta"


def test_arbitrary_attribute(ev):
    assert ev.string("id.hit@id") == "hit"


def test_missing_selector_returns_empty_not_error(ev):
    assert ev.string("class.nope@text") == ""
    assert ev.strings("class.nope@text") == []


def test_unknown_last_segment_is_treated_as_attribute(ev):
    """对齐 Legado：最后一段不是已知关键字时当**属性名**取，取不到就是空。

    裸标识符在语法上无法区分「标签名」和「属性名」（`@em` vs `@content`），
    Legado 的 `AnalyzeByJSoup.getResultLast` 走 `else -> elements.attr(lastRule)`，
    所以 `id.intro@em` 是 `attr("em")` → 空。要取 em 的文字得写 `id.intro@tag.em@text`。
    """
    assert ev.string("id.intro@em") == ""
    assert ev.string("id.intro@tag.em@text") == "很好看"
