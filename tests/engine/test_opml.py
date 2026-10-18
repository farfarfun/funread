"""OPML 解析与生成。离线样本，不打网。"""

import pytest

from funread.legado.engine import OpmlEntry, RuleSyntaxError, build_opml, parse_opml

INOREADER = """<?xml version="1.0" encoding="UTF-8"?>
<opml version="1.0">
  <head><title>Inoreader 导出</title></head>
  <body>
    <outline text="科技">
      <outline text="Hacker News" title="Hacker News" type="rss"
               xmlUrl="https://news.ycombinator.com/rss" htmlUrl="https://news.ycombinator.com/"/>
      <outline text="阮一峰" type="rss" xmlUrl="https://www.ruanyifeng.com/blog/atom.xml"/>
    </outline>
    <outline text="没分组的" type="rss" xmlUrl="https://blog.example.com/feed.xml"/>
    <outline text="重复的" type="rss" xmlUrl="https://BLOG.example.com/feed.xml"/>
  </body>
</opml>
"""


def test_entries_in_file_order_with_group_from_the_enclosing_outline():
    entries = parse_opml(INOREADER)
    assert [e.title for e in entries] == ["Hacker News", "阮一峰", "没分组的"]
    assert entries[0].group == "科技"
    assert entries[0].html_url == "https://news.ycombinator.com/"
    assert entries[2].group == ""


def test_the_same_url_is_kept_once_ignoring_case():
    """导出工具常把同一个源写两遍（分组里一遍、根下一遍）。"""
    urls = [e.xml_url.lower() for e in parse_opml(INOREADER)]
    assert len(urls) == len(set(urls))


def test_bytes_input_works():
    assert len(parse_opml(INOREADER.encode("utf-8"))) == 3


@pytest.mark.parametrize(
    "payload, reason",
    [
        ("", "为空"),
        ("<html><body>首页</body></html>", "HTML 页面"),
        ("<rss><channel/></rss>", "<rss>"),
        ("<opml><head/></opml>", "<body>"),
        ('<opml><body><outline text="只有分组"/></body></opml>', "xmlUrl"),
    ],
)
def test_wrong_files_are_rejected_with_a_reason(payload, reason):
    with pytest.raises(RuleSyntaxError) as info:
        parse_opml(payload)
    assert reason in str(info.value)


def test_build_then_parse_round_trips():
    entries = [
        OpmlEntry(xml_url="https://a.example.com/feed", title="A", group="科技"),
        OpmlEntry(xml_url="https://b.example.com/feed", title='B "引号" & 符号'),
        OpmlEntry(xml_url="https://c.example.com/feed", title="C", group="科技"),
    ]
    text = build_opml(entries)
    assert text.startswith('<?xml version="1.0"')
    back = parse_opml(text)
    assert [(e.xml_url, e.title, e.group) for e in back] == [
        ("https://b.example.com/feed", 'B "引号" & 符号', ""),
        ("https://a.example.com/feed", "A", "科技"),
        ("https://c.example.com/feed", "C", "科技"),
    ]


def test_build_with_nothing_is_still_a_valid_document():
    text = build_opml([])
    assert "<body>" in text and "</opml>" in text
