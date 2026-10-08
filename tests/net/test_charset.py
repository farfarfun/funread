"""编码判定。中文小说站上这一步错了，整页就是乱码 —— 比抓不到更难查。"""

import pytest

from funread.legado.net import charset

# ------------------------------------------------------------------ 规范化


@pytest.mark.parametrize(
    "declared,expected",
    [
        ("utf-8", "utf-8"),
        ("UTF-8", "utf-8"),
        ("  'utf-8'  ", "utf-8"),
        #  超集纠偏：声明 gb2312 但正文带生僻字/繁体的站点极多
        ("gb2312", "gb18030"),
        ("GBK", "gb18030"),
        ("gb_2312", "gb18030"),
        ("big5", "big5hkscs"),
        ("", ""),
        (None, ""),
        ("不存在的编码", ""),
    ],
)
def test_normalize(declared, expected):
    assert charset.normalize(declared) == expected


# ------------------------------------------------------------ Content-Type


def test_content_type_charset_used():
    assert charset.from_content_type("text/html; charset=gbk") == "gb18030"


def test_content_type_without_charset():
    assert charset.from_content_type("text/html") == ""


@pytest.mark.parametrize("declared", ["ISO-8859-1", "iso-8859-1", "us-ascii"])
def test_content_type_default_values_ignored(declared):
    """requests 在无声明时按 RFC 退回 ISO-8859-1 —— 这是服务器默认值，不是真实声明。"""
    assert charset.from_content_type(f"text/html; charset={declared}") == ""


def test_content_type_none():
    assert charset.from_content_type(None) == ""


# -------------------------------------------------------------------- meta


def test_meta_charset():
    raw = b'<html><head><meta charset="gbk"></head><body>x</body></html>'
    assert charset.from_meta(raw) == "gb18030"


def test_meta_http_equiv():
    raw = (
        b"<html><head><meta http-equiv='Content-Type' "
        b"content='text/html; charset=gb2312'></head></html>"
    )
    assert charset.from_meta(raw) == "gb18030"


def test_meta_only_scanned_in_head():
    """meta 只可能在文档头部；为了不扫全文，超出扫描窗口的声明就当没有。"""
    raw = b"<html><body>" + b"x" * 5000 + b'<meta charset="gbk">' + b"</body></html>"
    assert charset.from_meta(raw) == ""


def test_meta_absent():
    assert charset.from_meta(b"<html><body>x</body></html>") == ""


# ---------------------------------------------------------------- 优先级


def test_declared_wins_over_everything():
    raw = b'<meta charset="utf-8">'
    assert charset.resolve(raw, declared="gbk", content_type="text/html; charset=utf-8") == (
        "gb18030"
    )


def test_content_type_wins_over_meta():
    raw = b'<meta charset="utf-8">'
    assert charset.resolve(raw, content_type="text/html; charset=gbk") == "gb18030"


def test_meta_used_when_header_is_default():
    raw = b'<meta charset="gbk">'
    assert charset.resolve(raw, content_type="text/html; charset=ISO-8859-1") == "gb18030"


def test_falls_back_to_utf8():
    assert charset.resolve(b"", content_type="") == "utf-8"


# ------------------------------------------------------------------ 解码


def test_decode_gbk_page():
    raw = "第一章 惊蛰".encode("gb18030")
    text, encoding = charset.decode(raw, declared="gb2312")
    assert text == "第一章 惊蛰"
    assert encoding == "gb18030"


def test_decode_utf8_page_via_meta():
    raw = '<html><meta charset="utf-8"><body>剑来</body></html>'.encode()
    text, encoding = charset.decode(raw)
    assert "剑来" in text
    assert encoding == "utf-8"


def test_decode_bad_bytes_replaced_not_raised():
    """坏字节只换成替换符，不抛异常 —— 整章正文不该因为一个烂字节作废。

    注意坏字节会让多字节流失步，紧随其后的字可能一起糊掉，这是 gb18030 的固有
    性质，不是这里的缺陷；能保证的是「不炸」加「前面的内容完好」。
    """
    raw = "正文照常".encode("gb18030") + b"\xff"
    text, _ = charset.decode(raw, declared="gbk")
    assert text.startswith("正文照常")
    assert "�" in text


def test_decode_gb18030_only_char():
    """只有 gb18030 认的字：按声明的 gb2312 解会炸，纠偏后才读得出来。"""
    raw = "镕".encode("gb18030")
    text, _ = charset.decode(raw, declared="gb2312")
    assert text == "镕"
