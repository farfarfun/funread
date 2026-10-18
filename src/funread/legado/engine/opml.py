"""OPML 导入导出：订阅列表的通用交换格式。

Inoreader / FreshRSS / Folo / NetNewsWire 都用它迁移订阅，所以这是别的阅读器
的用户搬进来、以及从这里搬走的唯一通路。只认 `type="rss"` 的 outline（有
`xmlUrl` 的那些）；用来分组的 outline 没有 `xmlUrl`，它的 `text` 作为子项的
`group` 带下去。

遵守 `engine/` 的分层禁令：只用 lxml + stdlib，不抓网络，不碰数据库。导入时
**不验证**每个地址能不能抓 —— 一份 OPML 动辄上百条，逐条同步抓会让导入等很久，
而且里面必然有已经挂掉的源。先全部入库，失败的在第一次刷新时自然落到
`last_error`，那条路已经有了。
"""

from dataclasses import dataclass
from typing import List, Optional
from xml.sax.saxutils import quoteattr

from .errors import RuleSyntaxError


@dataclass
class OpmlEntry:
    """一条 feed 订阅。`group` 是它所在 outline 的名字，没有就是空串。"""

    xml_url: str
    title: str = ""
    html_url: str = ""
    group: str = ""


def _localname(tag: object) -> str:
    if not isinstance(tag, str):
        return ""
    return tag.rsplit("}", 1)[-1]


def _walk(node, group: str, out: List[OpmlEntry]) -> None:
    for child in node:
        if _localname(child.tag) != "outline":
            continue
        xml_url = (child.get("xmlUrl") or child.get("xmlurl") or "").strip()
        text = (child.get("text") or child.get("title") or "").strip()
        if xml_url:
            out.append(
                OpmlEntry(
                    xml_url=xml_url,
                    title=text,
                    html_url=(child.get("htmlUrl") or child.get("htmlurl") or "").strip(),
                    group=group,
                )
            )
        #  有 xmlUrl 的 outline 一般没有子节点，但真有的话也照走 ——
        #  分组名用它自己的 text，没有就沿用外层的
        _walk(child, text if not xml_url and text else group, out)


def parse_opml(payload) -> List[OpmlEntry]:
    """解析一份 OPML，返回里面的 feed 条目（按文件顺序，同一地址只保留第一条）。

    解析不出 `<opml>`/`<body>` 时抛 `RuleSyntaxError`，和 `parse_feed` 一样的
    理由：用户传错文件是最常见的误用，不能返回空列表让人以为「文件里没有订阅」。
    """
    from lxml import etree

    data = payload.encode("utf-8") if isinstance(payload, str) else payload
    if not (data or b"").strip():
        raise RuleSyntaxError("OPML 内容为空")

    parser = etree.XMLParser(recover=True, resolve_entities=False, no_network=True)
    try:
        root = etree.fromstring(data, parser=parser)
    except etree.XMLSyntaxError as error:
        raise RuleSyntaxError(f"不是合法的 XML：{error}") from error
    if root is None:
        raise RuleSyntaxError("无法解析为 XML，可能不是 OPML 文件")
    if _localname(root.tag).lower() == "html":
        raise RuleSyntaxError("这是一个 HTML 页面而不是 OPML 文件")
    if _localname(root.tag).lower() != "opml":
        raise RuleSyntaxError(f"根元素是 <{_localname(root.tag)}> 而不是 <opml>")

    body = next((child for child in root if _localname(child.tag) == "body"), None)
    if body is None:
        raise RuleSyntaxError("OPML 里没有 <body>")

    found: List[OpmlEntry] = []
    _walk(body, "", found)

    seen = set()
    entries: List[OpmlEntry] = []
    for entry in found:
        key = entry.xml_url.lower()
        if key in seen:
            continue
        seen.add(key)
        entries.append(entry)
    if not entries:
        raise RuleSyntaxError("这份 OPML 里没有任何带 xmlUrl 的订阅")
    return entries


def build_opml(entries: List[OpmlEntry], title: str = "funread 订阅") -> str:
    """生成 OPML 2.0。同组的条目收进一个分组 outline，没分组的平铺在最外层。

    用字符串拼而不是 lxml 建树：输出很小、结构固定，`quoteattr` 已经够保证
    属性值安全，没必要为此多一次树构建。
    """
    grouped = {}
    for entry in entries:
        grouped.setdefault(entry.group, []).append(entry)

    def line(entry: OpmlEntry, indent: str) -> str:
        attrs = [
            f"text={quoteattr(entry.title or entry.xml_url)}",
            'type="rss"',
            f"xmlUrl={quoteattr(entry.xml_url)}",
        ]
        if entry.html_url:
            attrs.append(f"htmlUrl={quoteattr(entry.html_url)}")
        return f"{indent}<outline {' '.join(attrs)}/>"

    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<opml version="2.0">',
        "  <head>",
        f"    <title>{_escape(title)}</title>",
        "  </head>",
        "  <body>",
    ]
    for entry in grouped.get("", []):
        lines.append(line(entry, "    "))
    for group, members in grouped.items():
        if not group:
            continue
        lines.append(f"    <outline text={quoteattr(group)}>")
        lines.extend(line(entry, "      ") for entry in members)
        lines.append("    </outline>")
    lines.extend(["  </body>", "</opml>", ""])
    return "\n".join(lines)


def _escape(text: Optional[str]) -> str:
    return (text or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


__all__ = ["OpmlEntry", "build_opml", "parse_opml"]
