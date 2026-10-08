"""响应编码判定。

**绝不使用 `response.text`。** requests 在没有 `charset` 的 Content-Type 上会按
RFC 2616 退回 ISO-8859-1，中文站点上这等于一整页乱码；它的 `apparent_encoding`
又会拉 chardet 做全文统计，慢且在短页面上不准。

判定顺序（先命中先用）：

1. 规则里 URL 选项给的 `charset` —— 源作者手写的，最权威
2. Content-Type 里的 `charset`，但**排除 ISO-8859-1 / US-ASCII** —— 它们基本都是
   服务器的默认值而非真实声明
3. HTML 里的 `<meta charset>` / `<meta http-equiv>`（只在头部几 KB 里找）
4. `charset_normalizer` 统计猜测
5. utf-8

另外做两个超集纠偏：`gb2312`/`gbk` → `gb18030`，`big5` → `big5hkscs`。
声明 gb2312 但正文里带生僻字/繁体的站点非常多，按声明解码会在半页处炸掉。
"""

import re
from typing import Optional

#  服务器默认值，不是真实声明，必须忽略
_IGNORED_DECLARED = {"iso-8859-1", "latin-1", "latin1", "us-ascii", "ascii"}

#  超集替换：声明的编码覆盖面不够，用兼容超集解码
_SUPERSET = {
    "gb2312": "gb18030",
    "gbk": "gb18030",
    "gb_2312": "gb18030",
    "euc-cn": "gb18030",
    "big5": "big5hkscs",
    "cp950": "big5hkscs",
}

_META_CHARSET_RE = re.compile(rb"""<meta[^>]+charset\s*=\s*["']?\s*([\w\-]+)""", re.I)
_META_HTTP_EQUIV_RE = re.compile(
    rb"""<meta[^>]+http-equiv\s*=\s*["']?content-type["']?[^>]+content\s*=\s*["'][^"']*charset\s*=\s*([\w\-]+)""",
    re.I,
)

#  meta 只可能在文档头部，不用扫全文
_META_SCAN_BYTES = 4096


def normalize(name: Optional[str]) -> str:
    """规范化编码名，并把窄编码换成兼容超集。认不出来的返回空串。"""
    if not name:
        return ""
    key = name.strip().strip("\"'").lower().replace("_", "-")
    key = _SUPERSET.get(key, _SUPERSET.get(key.replace("-", "_"), key))
    if not key:
        return ""
    import codecs

    try:
        codecs.lookup(key)
    except LookupError:
        return ""
    return key


def from_content_type(content_type: Optional[str]) -> str:
    """从 Content-Type 头里取 charset，忽略 ISO-8859-1 这类服务器默认值。"""
    if not content_type or "charset" not in content_type.lower():
        return ""
    match = re.search(r"charset\s*=\s*([^;\s]+)", content_type, re.I)
    if not match:
        return ""
    declared = match.group(1).strip().strip("\"'").lower()
    if declared in _IGNORED_DECLARED:
        return ""
    return normalize(declared)


def from_meta(raw: bytes) -> str:
    """从 HTML 头部的 `<meta>` 里嗅编码。"""
    head = raw[:_META_SCAN_BYTES]
    for pattern in (_META_HTTP_EQUIV_RE, _META_CHARSET_RE):
        match = pattern.search(head)
        if match:
            found = normalize(match.group(1).decode("ascii", "ignore"))
            if found:
                return found
    return ""


def detect(raw: bytes) -> str:
    """统计猜测。charset_normalizer 缺失时静默跳过（它在 parse extra 里）。"""
    if not raw:
        return ""
    try:
        from charset_normalizer import from_bytes
    except ImportError:  # pragma: no cover - 依赖缺失路径
        return ""
    best = from_bytes(raw).best()
    return normalize(best.encoding) if best is not None else ""


def resolve(
    raw: bytes,
    *,
    declared: str = "",
    content_type: str = "",
) -> str:
    """按优先级定下解码用的编码名。永远返回一个可用的编码。"""
    for candidate in (
        normalize(declared),
        from_content_type(content_type),
        from_meta(raw),
        detect(raw),
    ):
        if candidate:
            return candidate
    return "utf-8"


def decode(raw: bytes, *, declared: str = "", content_type: str = "") -> tuple:
    """解码响应体，返回 `(text, encoding)`。

    用 `errors="replace"`：个别坏字节不该让整章正文作废 —— 读者宁可看到一个
    替换符，也不想看到一片空白。
    """
    encoding = resolve(raw, declared=declared, content_type=content_type)
    return raw.decode(encoding, errors="replace"), encoding


__all__ = [
    "decode",
    "detect",
    "from_content_type",
    "from_meta",
    "normalize",
    "resolve",
]
