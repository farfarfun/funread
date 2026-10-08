"""lxml 上的 jsoup 语义实现。

几个必须对齐 jsoup、不能想当然的点：

- `class.foo` 是 **class token 匹配**，不是 `@class='foo'`：`class="a foo b"` 命中，
  `class="foobar"` 不命中。真实源里 name 还会带空格（`class.size16 color5`），
  按空格切成多个 token 逐个 contains。
- `text.关键字` 对齐 jsoup 的 `:containsOwn`（只看**直接子文本节点**），不是 `:contains`
  —— 后者会把 `<body>` 这种祖先也命中。
- `href` / `src` 取完要绝对化（对齐 jsoup 的 `absUrl`）。
"""

import re
from typing import Any, List, Optional, Tuple
from urllib.parse import urljoin

from ..errors import RuleSyntaxError
from ..nodes import Extractor, Step
from ..values import STRING_BACKEND, RuleValue

_BLOCK_TAGS = {
    "address",
    "article",
    "aside",
    "blockquote",
    "br",
    "dd",
    "div",
    "dl",
    "dt",
    "fieldset",
    "figcaption",
    "figure",
    "footer",
    "form",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "header",
    "hr",
    "li",
    "main",
    "nav",
    "ol",
    "p",
    "pre",
    "section",
    "table",
    "tbody",
    "td",
    "tfoot",
    "th",
    "thead",
    "tr",
    "ul",
}


def _lxml():
    try:
        import lxml.html
    except ImportError as exc:  # pragma: no cover - 依赖缺失路径
        raise RuleSyntaxError("HTML 规则需要 lxml，请安装：pip install 'funread[parse]'") from exc
    return lxml.html


def _translator():
    try:
        from cssselect import GenericTranslator
    except ImportError as exc:  # pragma: no cover - 依赖缺失路径
        raise RuleSyntaxError(
            "CSS 规则需要 cssselect，请安装：pip install 'funread[parse]'"
        ) from exc
    return GenericTranslator()


def parse_document(text: str):
    """整页解析。

    用 `document_fromstring` 而不是 `fromstring`：后者对「只有一个根元素」的片段会把
    那个元素本身当根返回，于是 `id.x`（`.//*[@id]`，只找后代）就找不到它。jsoup 的
    Document 永远有 html/body 包裹，这里必须对齐，否则短 HTML 上的规则会静默失配。

    传 str 而不是 bytes —— 编码已在 net 层定好，不让 lxml 二次猜。
    """
    html = _lxml()
    if not text or not text.strip():
        return html.document_fromstring("<html><body></body></html>")
    return html.document_fromstring(text)


def parse_fragment(text: str):
    """JSON 字段里的 HTML 片段。"""
    html = _lxml()
    if not text or not text.strip():
        return html.fromstring("<div></div>")
    return html.fragment_fromstring(text, create_parent="div")


def _is_element(node: Any) -> bool:
    return hasattr(node, "tag") and isinstance(getattr(node, "tag", None), str)


def _element_children(element: Any) -> List[Any]:
    return [child for child in element if _is_element(child)]


def _own_text_pieces(element: Any) -> List[str]:
    """直接子文本节点（element.text + 每个子元素的 tail）。"""
    pieces: List[str] = []
    if element.text:
        pieces.append(element.text)
    for child in element:
        if child.tail:
            pieces.append(child.tail)
    return pieces


def jsoup_text(element: Any) -> str:
    """对齐 jsoup 的 `Element.text()`：块级边界与 <br> 插空格，最后折叠空白。"""
    if not _is_element(element):
        return str(element).strip()
    import re

    parts: List[str] = []

    def walk(node: Any) -> None:
        tag = node.tag if isinstance(node.tag, str) else ""
        if tag in _BLOCK_TAGS:
            parts.append(" ")
        if node.text:
            parts.append(node.text)
        for child in node:
            if _is_element(child):
                walk(child)
            if child.tail:
                parts.append(child.tail)
        if tag in _BLOCK_TAGS:
            parts.append(" ")

    walk(element)
    return re.sub(r"\s+", " ", "".join(parts)).strip()


def _inner_html(element: Any) -> str:
    html = _lxml()
    pieces: List[str] = [element.text or ""]
    for child in element:
        pieces.append(html.tostring(child, encoding="unicode", method="html"))
    return "".join(pieces)


def _outer_html(element: Any) -> str:
    html = _lxml()
    return html.tostring(element, encoding="unicode", method="html")


#  能安全拼进 XPath 的标签名。`*` 放过（`tag.*` 是合法的通配）。
_TAG_NAME_RE = re.compile(r"\*|[A-Za-z_][\w-]*")


#  jsoup 的 `Element.select()` 从**根节点自身**开始遍历 —— 根匹配就算命中。
#  对齐这个语义很关键：行规则 `bookList = class.bookinfo` 把作用域收到 `.bookinfo`
#  上之后，字段规则还会再写一遍 `class.bookinfo@tag.a@href`（真实源里非常常见），
#  用 `.//` 的话根节点自己被排除在外，这类字段全取不到。cssselect 翻出来的 XPath
#  本来就是 `descendant-or-self::`，不统一的话两条路还会给出不同结果。
_SELF_AXIS = "descendant-or-self::"


def _class_token_xpath(name: str) -> str:
    tokens = [tok for tok in name.split() if tok]
    if not tokens:
        return f"{_SELF_AXIS}*"
    conds = " and ".join(
        f"contains(concat(' ', normalize-space(@class), ' '), ' {tok} ')" for tok in tokens
    )
    return f"{_SELF_AXIS}*[{conds}]"


class HtmlBackend:
    name = "html"

    def select(self, items: Tuple, step: Step, base_url: str) -> Tuple:
        out: List[Any] = []
        for element in items:
            if not _is_element(element):
                continue
            out.extend(self._select_one(element, step))
        # 去重保序（多个父节点可能命中同一个后代）
        unique: List[Any] = []
        for node in out:
            if not any(node is existing for existing in unique):
                unique.append(node)
        return tuple(unique)

    def _select_one(self, element: Any, step: Step) -> List[Any]:
        if step.kind in {"class", "id", "tag", "text"} and not step.name:
            # `id.` / `tag.` 这类空名字定位步 —— `@get:{}` 没取到值时的残留。空结果，不报错。
            return []
        if step.kind == "class":
            return element.xpath(_class_token_xpath(step.name))
        if step.kind == "id":
            return element.xpath(f"{_SELF_AXIS}*[@id={_quote(step.name)}]")
        if step.kind == "tag":
            if not _TAG_NAME_RE.fullmatch(step.name):
                #  `tag.a:eq(0)` / `tag.a.b` 这种名字直接拼进 XPath 会抛
                #  XPathEvalError。标签名不合法 = 没命中，别把整条流程带崩。
                return []
            return element.xpath(f"{_SELF_AXIS}{step.name}")
        if step.kind == "children":
            return _element_children(element)
        if step.kind == "index":
            return [element]
        if step.kind == "text":
            keyword = step.name
            return [
                node
                for node in element.xpath(f"{_SELF_AXIS}*")
                if keyword in "".join(_own_text_pieces(node))
            ]
        if step.kind == "css":
            try:
                xpath = _translator().css_to_xpath(step.name)
            except Exception as exc:
                if step.lenient:
                    #  Default 方言猜出来的 CSS（`tr!0` 这种）：猜错就是没命中。
                    #  jsoup 还有 `:eq()`/`:containsOwn()` 这类 cssselect 不认的伪类，
                    #  为它们抛错会把整个源判死。
                    return []
                raise RuleSyntaxError(f"CSS 选择器无法翻译：{step.name!r}") from exc
            return element.xpath(xpath)
        raise RuleSyntaxError(f"未知的定位步骤类型：{step.kind}")

    def extract(self, items: Tuple, extractor: Extractor, base_url: str) -> List[str]:
        kind = extractor.kind
        out: List[str] = []
        for element in items:
            if kind == "text":
                out.append(jsoup_text(element))
            elif kind == "textNodes":
                pieces = [p.strip() for p in _own_text_pieces(element)]
                out.append("\n".join(p for p in pieces if p))
            elif kind == "ownText":
                import re

                joined = " ".join(_own_text_pieces(element))
                out.append(re.sub(r"\s+", " ", joined).strip())
            elif kind == "html":
                out.append(_inner_html(element))
            elif kind in ("outerHtml", "all"):
                out.append(_outer_html(element))
            elif kind == "attr":
                value = element.get(extractor.name) or ""
                if extractor.name in ("href", "src") and value:
                    value = _absolutize(base_url, value)
                out.append(value)
            elif kind == "group":
                out.append(jsoup_text(element))
            else:
                raise RuleSyntaxError(f"HTML 不支持的取值方式：{kind}")
        return out

    def to_string(self, item: Any) -> str:
        if _is_element(item):
            return jsoup_text(item)
        return str(item)

    def child_value(self, item: Any) -> RuleValue:
        if _is_element(item):
            return RuleValue(backend=self, items=(item,))
        return RuleValue(backend=STRING_BACKEND, items=(str(item),))


def _quote(value: str) -> str:
    if "'" not in value:
        return f"'{value}'"
    if '"' not in value:
        return f'"{value}"'
    parts = value.split("'")
    joined = ', "\'", '.join(f"'{p}'" for p in parts)
    return f"concat({joined})"


def _absolutize(base_url: str, url: str) -> str:
    url = url.strip()
    if not url:
        return ""
    if url.startswith(("http://", "https://", "data:", "javascript:", "mailto:")):
        return url
    if url.startswith("//"):
        from urllib.parse import urlsplit

        scheme = urlsplit(base_url).scheme or "https"
        return f"{scheme}:{url}"
    if not base_url:
        return url
    return urljoin(base_url, url)


HTML_BACKEND = HtmlBackend()


def document_value(text: str, *, fragment: bool = False) -> RuleValue:
    root = parse_fragment(text) if fragment else parse_document(text)
    return RuleValue(backend=HTML_BACKEND, items=(root,))


def xpath_select(items: Tuple, expression: str) -> Optional[RuleValue]:
    """XPath 方言：结果可能是元素，也可能是属性/文本（字符串）。"""
    collected: List[Any] = []
    for element in items:
        if not _is_element(element):
            continue
        try:
            collected.extend(element.xpath(expression))
        except Exception as exc:
            raise RuleSyntaxError(f"XPath 表达式无法求值：{expression!r}") from exc
    if not collected:
        return RuleValue.empty()
    if all(_is_element(node) for node in collected):
        return RuleValue(backend=HTML_BACKEND, items=tuple(collected))
    return RuleValue(backend=STRING_BACKEND, items=tuple(str(node) for node in collected))


__all__ = [
    "HTML_BACKEND",
    "HtmlBackend",
    "document_value",
    "jsoup_text",
    "parse_document",
    "parse_fragment",
    "xpath_select",
]
