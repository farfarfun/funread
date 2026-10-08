"""组合符 `&& || %%` 与正则三形态。"""

import pytest

from funread.legado.engine import RuleEvaluator

HTML = """
<html><body>
  <div id="a"><i>a1</i><i>a2</i></div>
  <div id="b"><i>b1</i><i>b2</i></div>
  <div id="empty"></div>
  <div id="kind">分类： 玄幻</div>
  <div id="body">正文第一段
本章未完，请点击下一页继续阅读
正文第二段</div>
  <ul id="chapters">
    <li><a href="/c/1">第一章</a></li>
    <li><a href="/c/2">第二章</a></li>
  </ul>
</body></html>
"""


@pytest.fixture
def ev():
    return RuleEvaluator(HTML, base_url="https://example.com/")


def test_and_merges_in_order(ev):
    assert ev.strings("id.a@tag.i@text&&id.b@tag.i@text") == ["a1", "a2", "b1", "b2"]


def test_or_takes_first_non_empty(ev):
    assert ev.strings("id.empty@tag.i@text||id.b@tag.i@text") == ["b1", "b2"]
    assert ev.strings("id.a@tag.i@text||id.b@tag.i@text") == ["a1", "a2"]


def test_interleave(ev):
    assert ev.strings("id.a@tag.i@text%%id.b@tag.i@text") == ["a1", "b1", "a2", "b2"]


def test_and_does_not_dedupe(ev):
    """`&&` 是纯拼接，重复值要保留（对齐 Legado 的 addAll）。

    去重会误伤合法重复 —— 章节名列表里本来就可能有两个同名章节。
    """
    assert ev.strings("id.a@tag.i@text&&id.a@tag.i@text") == ["a1", "a2", "a1", "a2"]


def test_select_dedupes_within_one_step(ev):
    """同一步定位里多个父节点命中同一后代时要去重，这跟 `&&` 不是一回事。"""
    assert ev.strings("tag.div@tag.i@text") == ["a1", "a2", "b1", "b2"]


def test_purify_strips_prefix(ev):
    assert ev.string("id.kind@text##分类：\\s*") == "玄幻"


def test_purify_with_alternation(ev):
    """净化正则里带 `|` 的多选分支（真实源里很常见）。"""
    out = ev.string("id.body@textNodes## |本章未完.*")
    assert "本章未完" not in out
    assert "正文第一段" in out and "正文第二段" in out


def test_only_one_replaces_first_match():
    ev = RuleEvaluator("<div id=x>aXbXc</div>", base_url="")
    assert ev.string("id.x@text##X##-###") == "a-bXc"


def test_purify_replaces_all():
    ev = RuleEvaluator("<div id=x>aXbXc</div>", base_url="")
    assert ev.string("id.x@text##X##-") == "a-b-c"


def test_all_in_one_regex_rows_and_groups():
    ev = RuleEvaluator(HTML, base_url="https://example.com/")
    rows = ev.rows(r':<li><a href="(.*?)">(.*?)</a></li>')
    assert len(rows) == 2
    assert [row.string("$2") for row in rows] == ["第一章", "第二章"]
    assert [row.url("$1") for row in rows] == [
        "https://example.com/c/1",
        "https://example.com/c/2",
    ]


def test_dollar_ref_in_purify_replacement():
    ev = RuleEvaluator("<div id=x>第12章 标题</div>", base_url="")
    assert ev.string("id.x@text##第(\\d+)章.*##$1") == "12"
