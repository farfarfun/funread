"""标准 feed 解析：RSS 2.0 / Atom / RDF。

这是订阅源的「第二条腿」。Legado 归档里纯 Python 能跑的只有 10.6%（1,344 个里
142 个），光靠那些源，C 端的订阅页内容太薄。而用户自己贴一个 `/feed.xml` 走这
条路径是 **100% 可用**的 —— 标准 feed 就是结构化 XML，没有规则、没有 JS。

不引 feedparser：要的只是「取 item 列表加五个字段」，lxml 已经在依赖里了，
而 feedparser 会带来一整套它自己的容错语义和一个新依赖。

遵守 `engine/` 的分层禁令：只用 lxml + stdlib，不抓网络（抓取是 `reader/` 的事），
输入是已经拿到的 bytes/str。
"""

import re
from typing import List, Optional

from .errors import RuleSyntaxError
from .models import RssArticle, RssPage

#: 认得的命名空间。feed 里 Atom 元素常带命名空间，RSS 2.0 的核心元素不带。
_NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rss1": "http://purl.org/rss/1.0/",
    "content": "http://purl.org/rss/1.0/modules/content/",
    "dc": "http://purl.org/dc/elements/1.1/",
    "media": "http://search.yahoo.com/mrss/",
}

#: `<img src=...>` 第一张图，给没有 enclosure/media:thumbnail 的 feed 兜底。
_IMG_RE = re.compile(r"""<img[^>]+src\s*=\s*["']([^"']+)["']""", re.I)


def _localname(tag: object) -> str:
    """去掉命名空间的标签名。注释/PI 节点的 tag 不是字符串，返回空串。"""
    if not isinstance(tag, str):
        return ""
    return tag.rsplit("}", 1)[-1]


def _text_of(element) -> str:
    """元素的文字内容，含子元素里的文字，压掉多余空白。"""
    if element is None:
        return ""
    joined = "".join(element.itertext())
    return re.sub(r"\s+", " ", joined).strip()


def _first(parent, *names: str):
    """按 localname 找第一个直接子元素，忽略命名空间。

    按给定顺序找，所以调用处的参数顺序就是优先级。
    """
    for name in names:
        for child in parent:
            if _localname(child.tag) == name:
                return child
    return None


def _children(parent, name: str):
    """同名的全部直接子元素，忽略命名空间。`media:content` 可以出现多次。"""
    return [child for child in parent if _localname(child.tag) == name]


def _link_of(entry, base_url: str = "") -> str:
    """文章链接。

    RSS 用 `<link>文本</link>`，Atom 用 `<link rel="alternate" href=...>`。
    Atom 里 `rel="self"`/`rel="enclosure"` 不是文章地址，必须挑出 alternate
    （或没有 rel 的那个，按规范等价于 alternate）。
    """
    candidates = [child for child in entry if _localname(child.tag) == "link"]
    for child in candidates:
        rel = (child.get("rel") or "alternate").strip().lower()
        href = (child.get("href") or "").strip()
        if href and rel == "alternate":
            return href
    for child in candidates:
        text = _text_of(child)
        if text:
            return text
    #  有些 feed 只给 guid，而 guid 常常就是永久链接
    guid = _first(entry, "guid", "id")
    text = _text_of(guid)
    return text if text.startswith(("http://", "https://")) else ""


def _media_kind(node) -> str:
    """`media:content` / `enclosure` 节点的媒体大类：image / audio / video / 空。

    先看 MIME（`type="video/mp4"`），再看 MRSS 自己的 `medium="video"`。两个
    都没有就返回空 —— 调用方按「不知道是什么」处理，而不是猜。
    """
    if node is None:
        return ""
    mime = (node.get("type") or "").lower().strip()
    if "/" in mime:
        return mime.split("/", 1)[0]
    return (node.get("medium") or "").lower().strip()


def _image_of(entry, description: str, content: str) -> str:
    """配图。依次试 media:thumbnail / media:content / enclosure / 正文首图。

    `media:content` 与 `enclosure` 都要先看类型：它们装的可能是视频或音频，
    那个 URL 塞进 `<img>` 只会得到一张坏图。类型缺失的 `enclosure` 仍按图片处理
    （旧 feed 常省略 type），但 `media:content` 没有 type 也没有 medium 的就跳过
    —— MRSS 里它更常是视频。
    """
    node = _first(entry, "thumbnail")
    if node is not None and node.get("url"):
        return node.get("url") or ""
    for node in _children(entry, "content"):
        if node.get("url") and _media_kind(node) == "image":
            return node.get("url") or ""
    enclosure = _first(entry, "enclosure")
    if enclosure is not None:
        url = (enclosure.get("url") or "").strip()
        if url and _media_kind(enclosure) in ("", "image"):
            return url
    for body in (content, description):
        found = _IMG_RE.search(body or "")
        if found:
            return found.group(1)
    return ""


def _media_of(entry) -> tuple:
    """附件里的音频或视频：`(url, mime)`。没有就 `("", "")`。

    播客的音频地址一直就在 `<enclosure>` 里，早先 `_image_of` 拿到它又因为不是
    `image/*` 扔掉了。这里把它接住。只认 audio 与 video —— 其它类型（pdf 一类）
    不是「能在阅读器里播」的东西。
    """
    candidates = list(_children(entry, "enclosure")) + list(_children(entry, "content"))
    for node in candidates:
        url = (node.get("url") or "").strip()
        if url and _media_kind(node) in ("audio", "video"):
            return url, (node.get("type") or "").lower().strip()
    #  Atom 的 enclosure 是 <link rel="enclosure" href=... type=...>
    for node in _children(entry, "link"):
        if (node.get("rel") or "") == "enclosure":
            url = (node.get("href") or "").strip()
            if url and _media_kind(node) in ("audio", "video"):
                return url, (node.get("type") or "").lower().strip()
    return "", ""


def _content_of(entry) -> str:
    """正文 HTML。

    优先级 `content:encoded` > Atom `content` > `description`/`summary`：
    前两者是全文，`description` 在多数 feed 里只是摘要。注意不要压空白 ——
    这是 HTML，交给渲染端。
    """
    for name in ("encoded", "content", "description", "summary"):
        node = _first(entry, name)
        if node is None:
            continue
        #  Atom 的 content 可能是 XHTML 子树而不是文字
        inner = "".join(part if isinstance(part, str) else "" for part in (node.text or "",))
        if inner.strip():
            return inner
        if len(node):
            from lxml import etree

            return "".join(
                etree.tostring(child, encoding="unicode", method="html") for child in node
            ).strip()
    return ""


def _description_of(entry) -> str:
    node = _first(entry, "description", "summary", "subtitle")
    return _text_of(node)


def _pub_date_of(entry) -> str:
    """发布时间，原样返回不解析成 datetime。

    feed 里的时间格式有 RFC 822、ISO 8601 和各种手写变体。前端只需要展示，
    所以不在这里做必然会漏格式的归一化 —— 存原文，要排序时再说。
    """
    node = _first(entry, "pubDate", "published", "updated", "date", "created")
    return _text_of(node)


def parse_feed(payload, *, source_url: str = "") -> RssPage:
    """解析一段 feed，返回文章列表。

    认 RSS 2.0（`rss/channel/item`）、Atom（`feed/entry`）与 RSS 1.0/RDF
    （`RDF/item`）。解析不出任何条目时抛 `RuleSyntaxError` —— 用户贴了个 HTML
    页面当 feed 是最常见的误用，必须说清楚，不能返回空列表让人以为「这个源没
    更新」。
    """
    from lxml import etree

    data = payload.encode("utf-8") if isinstance(payload, str) else payload
    if not (data or b"").strip():
        raise RuleSyntaxError("feed 内容为空")

    #  recover=True：真实 feed 里未转义的 & 和不合法字符很常见，
    #  严格模式会把本来能读的 feed 整个判废。
    parser = etree.XMLParser(recover=True, resolve_entities=False, no_network=True)
    try:
        root = etree.fromstring(data, parser=parser)
    except etree.XMLSyntaxError as error:
        raise RuleSyntaxError(f"不是合法的 XML：{error}") from error
    if root is None:
        raise RuleSyntaxError("无法解析为 XML，可能不是 feed 地址")

    root_name = _localname(root.tag).lower()
    if root_name == "html":
        raise RuleSyntaxError("这是一个 HTML 页面而不是 feed 地址")

    channel = _first(root, "channel")
    container = channel if channel is not None else root
    entries = [child for child in container if _localname(child.tag) in ("item", "entry")]
    if not entries and channel is not None:
        #  RSS 1.0/RDF 把 item 放在 RDF 根下，不在 channel 里
        entries = [child for child in root if _localname(child.tag) == "item"]

    if not entries:
        raise RuleSyntaxError("这个地址里没有 item/entry，可能不是 feed")

    feed_title = _text_of(_first(container, "title"))
    articles: List[RssArticle] = []
    for entry in entries:
        title = _text_of(_first(entry, "title"))
        content = _content_of(entry)
        description = _description_of(entry)
        if not title.strip() and not content.strip():
            continue
        media_url, media_type = _media_of(entry)
        articles.append(
            RssArticle(
                title=title,
                link=_link_of(entry),
                pub_date=_pub_date_of(entry),
                description=description,
                image=_image_of(entry, description, content),
                media_url=media_url,
                media_type=media_type,
                content=content,
                source_url=source_url,
                source_name=feed_title,
            )
        )
    if not articles:
        raise RuleSyntaxError("feed 里的条目都没有标题和正文")
    return RssPage(items=articles)


def feed_title(payload, default: str = "") -> str:
    """只取 feed 标题，给「订阅时自动填名字」用。解析不动就返回 `default`。"""
    try:
        return parse_feed(payload).items[0].source_name or default
    except (RuleSyntaxError, IndexError):
        return default


def looks_like_feed(payload: Optional[bytes]) -> bool:
    """便宜的预检：这段内容像不像 feed。订阅前先过一道，省掉一次完整解析。"""
    head = (payload or b"")[:2048].lower()
    if isinstance(head, str):  # pragma: no cover - 调用方一般给 bytes
        head = head.encode("utf-8", "ignore")
    return any(marker in head for marker in (b"<rss", b"<feed", b"<rdf:rdf", b"<channel"))


__all__ = ["feed_title", "looks_like_feed", "parse_feed"]
