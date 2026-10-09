"""标准 feed 解析：RSS 2.0 / Atom / RDF。离线样本，不打网。"""

import pytest

from funread.legado.engine import RuleSyntaxError, feed_title, looks_like_feed, parse_feed

RSS_2 = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">
  <channel>
    <title>示例博客</title>
    <link>https://blog.example.com</link>
    <item>
      <title>第一篇</title>
      <link>https://blog.example.com/1</link>
      <pubDate>Wed, 01 Oct 2026 12:00:00 +0800</pubDate>
      <description>第一篇的摘要</description>
      <content:encoded><![CDATA[<p>第一篇的<b>全文</b></p>]]></content:encoded>
    </item>
    <item>
      <title>第二篇</title>
      <link>https://blog.example.com/2</link>
      <description>第二篇只有摘要 &amp; 没有全文</description>
      <enclosure url="https://blog.example.com/cover.png" type="image/png"/>
    </item>
  </channel>
</rss>
"""

ATOM = """<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>阮一峰的网络日志</title>
  <link rel="self" href="https://www.example.com/atom.xml"/>
  <entry>
    <title>周刊第一期</title>
    <link rel="alternate" href="https://www.example.com/weekly/1"/>
    <link rel="enclosure" href="https://www.example.com/audio.mp3"/>
    <published>2026-10-01T04:00:00Z</published>
    <summary>第一期摘要</summary>
    <content type="html">&lt;p&gt;第一期全文&lt;/p&gt;</content>
  </entry>
</feed>
"""

RDF = """<?xml version="1.0"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns="http://purl.org/rss/1.0/">
  <channel><title>老式站点</title></channel>
  <item>
    <title>一篇老文章</title>
    <link>https://old.example.com/1</link>
    <description>摘要</description>
  </item>
</rdf:RDF>
"""


# ---------------------------------------------------------------- RSS 2.0


def test_rss_2_items():
    page = parse_feed(RSS_2, source_url="https://blog.example.com/feed.xml")

    assert [a.title for a in page.items] == ["第一篇", "第二篇"]
    first = page.items[0]
    assert first.link == "https://blog.example.com/1"
    assert first.pub_date == "Wed, 01 Oct 2026 12:00:00 +0800"
    assert first.description == "第一篇的摘要"
    assert "全文" in first.content
    assert first.source_url == "https://blog.example.com/feed.xml"
    assert first.source_name == "示例博客"


def test_content_encoded_beats_description():
    """description 在多数 feed 里只是摘要，content:encoded 才是全文。"""
    first = parse_feed(RSS_2).items[0]
    assert "<b>全文</b>" in first.content
    assert first.description == "第一篇的摘要"


def test_description_is_used_when_there_is_no_full_text():
    second = parse_feed(RSS_2).items[1]
    assert "只有摘要" in second.content


def test_escaped_entities_are_decoded():
    assert "&" in parse_feed(RSS_2).items[1].description


def test_image_from_an_enclosure():
    assert parse_feed(RSS_2).items[1].image == "https://blog.example.com/cover.png"


# ---------------------------------------------------------------- Atom


def test_atom_entries():
    page = parse_feed(ATOM)
    entry = page.items[0]
    assert entry.title == "周刊第一期"
    assert entry.pub_date == "2026-10-01T04:00:00Z"
    assert entry.source_name == "阮一峰的网络日志"
    assert "第一期全文" in entry.content


def test_atom_picks_the_alternate_link_not_self_or_enclosure():
    """rel=self 是 feed 自己的地址，rel=enclosure 是附件 —— 都不是文章地址。"""
    assert parse_feed(ATOM).items[0].link == "https://www.example.com/weekly/1"


# ---------------------------------------------------------------- RDF


def test_rss_1_rdf_items_live_outside_the_channel():
    page = parse_feed(RDF)
    assert [a.title for a in page.items] == ["一篇老文章"]
    assert page.items[0].link == "https://old.example.com/1"


# ---------------------------------------------------------------- 容错


def test_a_link_only_in_guid_is_used():
    xml = """<rss version="2.0"><channel><item>
      <title>只有 guid</title>
      <guid isPermaLink="true">https://g.example.com/1</guid>
    </item></channel></rss>"""
    assert parse_feed(xml).items[0].link == "https://g.example.com/1"


def test_a_non_url_guid_is_not_mistaken_for_a_link():
    xml = """<rss version="2.0"><channel><item>
      <title>guid 不是链接</title><guid>abc-123</guid>
    </item></channel></rss>"""
    assert parse_feed(xml).items[0].link == ""


def test_an_unescaped_ampersand_does_not_kill_the_whole_feed():
    """真实 feed 里未转义的 & 很常见，严格模式会把能读的 feed 整个判废。"""
    xml = """<rss version="2.0"><channel><item>
      <title>A & B</title><link>https://x.example.com/1</link>
    </item></channel></rss>"""
    assert len(parse_feed(xml).items) == 1


def test_an_item_without_a_title_or_body_is_dropped():
    xml = """<rss version="2.0"><channel>
      <item><link>https://x.example.com/1</link></item>
      <item><title>有标题</title></item>
    </channel></rss>"""
    assert [a.title for a in parse_feed(xml).items] == ["有标题"]


def test_an_image_is_recovered_from_the_body():
    xml = """<rss version="2.0"><channel><item>
      <title>带图</title>
      <description><![CDATA[<p><img src="https://x.example.com/a.png"></p>]]></description>
    </item></channel></rss>"""
    assert parse_feed(xml).items[0].image == "https://x.example.com/a.png"


def test_bytes_input_works():
    assert len(parse_feed(RSS_2.encode("utf-8")).items) == 2


# ---------------------------------------------------------------- 错误


def test_an_html_page_is_rejected_with_a_clear_reason():
    """用户贴了个网页当 feed 是最常见的误用，必须说清楚。"""
    with pytest.raises(RuleSyntaxError, match="HTML 页面"):
        parse_feed("<html><body><h1>首页</h1></body></html>")


def test_empty_input_is_rejected():
    with pytest.raises(RuleSyntaxError, match="为空"):
        parse_feed("   ")


def test_xml_without_items_is_rejected():
    with pytest.raises(RuleSyntaxError, match="没有 item/entry"):
        parse_feed("<rss version='2.0'><channel><title>空的</title></channel></rss>")


def test_a_feed_whose_items_are_all_blank_is_rejected():
    with pytest.raises(RuleSyntaxError, match="都没有标题和正文"):
        parse_feed("<rss version='2.0'><channel><item><guid>x</guid></item></channel></rss>")


def test_errors_are_not_silently_empty_lists():
    """返回空列表会让人以为「这个源没更新」。"""
    for payload in ("", "<html></html>", "not xml at all"):
        with pytest.raises(RuleSyntaxError):
            parse_feed(payload)


# ---------------------------------------------------------------- 辅助


def test_feed_title_for_autofilling_a_subscription_name():
    assert feed_title(RSS_2) == "示例博客"
    assert feed_title(ATOM) == "阮一峰的网络日志"


def test_feed_title_falls_back_when_it_cannot_parse():
    assert feed_title("<html></html>", default="未命名") == "未命名"


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (b"<?xml version='1.0'?><rss version='2.0'>", True),
        (b"<feed xmlns='http://www.w3.org/2005/Atom'>", True),
        (b"<rdf:RDF ", True),
        (b"<!doctype html><html>", False),
        (b"", False),
        (None, False),
    ],
)
def test_looks_like_feed(payload, expected):
    assert looks_like_feed(payload) is expected
