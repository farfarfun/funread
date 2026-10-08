"""规则求值主循环。

对外只有 `RuleEvaluator`。它的 `rows()` 返回**子 RuleEvaluator** 而不是裸元素，
这样「HTML 元素行 / JSON 对象行 / 正则 match 行」三种完全不同的中间值在编排层
表现一致，四段流程的代码只写一次。
"""

from typing import Any, Dict, List, Literal, Optional, Sequence
from urllib.parse import urljoin, urlsplit

from .backends.html import HTML_BACKEND, xpath_select
from .backends.html import document_value as html_value
from .backends.jsonx import JSON_BACKEND, jsonpath_select
from .backends.jsonx import document_value as json_value
from .backends.regexrow import regex_rows
from .errors import RuleEmptyError, RuleSyntaxError
from .expr import MiniExpr
from .js import JsContext, JsRuntime, NullJsRuntime
from .lexer import (
    has_get_reference,
    parse_rule,
    split_template,
    substitute_get,
)
from .nodes import (
    AllInOneRegexAtom,
    Atom,
    ConstAtom,
    CssAtom,
    DefaultAtom,
    Extractor,
    JsAtom,
    JsonPathAtom,
    PipelineAtom,
    RuleProgram,
    TemplateAtom,
    XPathAtom,
)
from .values import RuleValue, string_value
from .variables import VariableScope

ContentType = Literal["html", "json", "xml", "text", "auto"]

_FALSY = {"", "null", "none", "false", "0", "no"}


def _looks_json(text: str) -> bool:
    stripped = text.lstrip()
    return stripped.startswith("{") or stripped.startswith("[")


class RuleEvaluator:
    """一个求值作用域：一整页，或列表里的一行。"""

    def __init__(
        self,
        content: Any,
        *,
        base_url: str = "",
        content_type: ContentType = "auto",
        variables: Optional[VariableScope] = None,
        js: Optional[JsRuntime] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        self._base_url = base_url or ""
        self._js = js or NullJsRuntime()
        self._context = dict(context or {})
        self._variables = variables if variables is not None else VariableScope()

        if isinstance(content, RuleValue):
            self._value = content
            self._raw_text: Optional[str] = None
        else:
            text = content if isinstance(content, str) else str(content)
            self._raw_text = text
            self._value = self._build_root(text, content_type)

    # ------------------------------------------------------------------ 构造

    def _build_root(self, text: str, content_type: ContentType) -> RuleValue:
        if content_type == "auto":
            content_type = "json" if _looks_json(text) else "html"
        if content_type == "json":
            try:
                return json_value(text)
            except RuleSyntaxError:
                return html_value(text)
        if content_type == "text":
            return string_value(text)
        return html_value(text)

    def _spawn(self, value: RuleValue, *, variables: VariableScope) -> "RuleEvaluator":
        child = RuleEvaluator.__new__(RuleEvaluator)
        child._base_url = self._base_url
        child._js = self._js
        child._context = self._context
        child._variables = variables
        child._value = value
        child._raw_text = None
        return child

    # ------------------------------------------------------------------ 属性

    @property
    def base_url(self) -> str:
        return self._base_url

    @property
    def variables(self) -> VariableScope:
        return self._variables

    @property
    def value(self) -> RuleValue:
        return self._value

    @property
    def text(self) -> str:
        """当前作用域的原始文本（AllInOne 正则和 sourceRegex 要用）。"""
        if self._raw_text is not None:
            return self._raw_text
        return self._value.as_string()

    # ------------------------------------------------------------------ 对外 API

    def rows(self, rule: Optional[str]) -> List["RuleEvaluator"]:
        """列表规则：每行一个子 evaluator，行级变量作用域互相隔离。"""
        if not rule or not rule.strip():
            return []
        value = self._eval_rule(rule, extract=False)
        return [self._spawn(row, variables=self._variables.child()) for row in value.rows()]

    def scope(self, rule: Optional[str]) -> Optional["RuleEvaluator"]:
        """把作用域收缩到规则命中的节点上（`ruleBookInfo.init` 的预处理入口）。

        必须走 `extract=False`：`value_of("id.detail")` 会按默认的 `text` 取值，
        拿到的是整块纯文本，再往下 `id.intro` 就什么都找不到了。
        没命中就返回 None，调用方保持原作用域。
        """
        if not rule or not rule.strip():
            return None
        value = self._eval_rule(rule, extract=False)
        if not value.items:
            return None
        return self._spawn(value, variables=self._variables)

    def template(self, text: Optional[str]) -> str:
        """字符串模板求值：只展开 `{{}}` / `<js></js>` / `@get:{}`，其余原样保留。

        这是 URL 字段（`searchUrl`/`exploreUrl`/`sortUrl`/URL 选项里的 `body`）的入口，
        **不是**选择器求值 —— `/search?q={{key}}` 里的 `/search?q=` 是字面量，
        丢给 `string()` 会被当成 CSS 选择器然后求出空串。
        """
        if not text:
            return ""
        if has_get_reference(text):
            text = substitute_get(text, self._variables.get)
        return self._expand_template(split_template(text))

    def _expand_template(self, parts) -> str:
        out: List[str] = []
        for kind, piece in parts:
            if kind == "lit":
                out.append(piece)
                continue
            resolved = self._expand_interpolation(piece)
            if resolved is None:
                result = self._js.eval(piece, self._js_context())
                resolved = "" if result is None else str(result)
            out.append(resolved)
        return "".join(out)

    def value_of(self, rule: Optional[str]) -> RuleValue:
        if not rule or not rule.strip():
            return RuleValue.empty()
        return self._eval_rule(rule, extract=True)

    def string(self, rule: Optional[str], *, default: str = "", sep: str = "\n") -> str:
        value = self.value_of(rule)
        if value.is_empty():
            return default
        return value.as_string(sep=sep)

    def strings(self, rule: Optional[str]) -> List[str]:
        value = self.value_of(rule)
        return [s for s in (s.strip() for s in value.as_strings()) if s]

    def url(self, rule: Optional[str]) -> str:
        raw = self.string(rule)
        return join_url(self._base_url, raw.strip().splitlines()[0]) if raw.strip() else ""

    def urls(self, rule: Optional[str]) -> List[str]:
        return [join_url(self._base_url, item) for item in self.strings(rule) if item]

    def flag(self, rule: Optional[str]) -> bool:
        """isVip / canReName 这类布尔语义。"""
        if not rule or not rule.strip():
            return False
        raw = self.string(rule).strip().lower()
        return raw not in _FALSY

    def require(self, rule: Optional[str], field: str) -> str:
        text = self.string(rule)
        if not text.strip():
            raise RuleEmptyError(f"必填字段 {field} 求值为空", rule=rule)
        return text

    # ------------------------------------------------------------------ 求值

    def _eval_rule(self, rule: str, *, extract: bool) -> RuleValue:
        if has_get_reference(rule):
            rule = substitute_get(rule, self._variables.get)
        program = parse_rule(rule)
        return self._eval_program(program, extract=extract)

    def _eval_program(self, program: RuleProgram, *, extract: bool) -> RuleValue:
        and_parts: List[RuleValue] = []
        for and_group in program.groups:
            chosen: Optional[RuleValue] = None
            for or_item in and_group:
                pieces = [self._eval_atom(atom, extract=extract) for atom in or_item]
                merged = RuleValue.interleave(pieces) if len(pieces) > 1 else pieces[0]
                if not merged.is_empty():
                    chosen = merged
                    break
            if chosen is not None:
                and_parts.append(chosen)

        out = RuleValue.concat(and_parts) if and_parts else RuleValue.empty()

        for key, sub_program in program.puts:
            captured = self._eval_program(sub_program, extract=True)
            self._variables.put(key, captured.as_string())

        if program.purify is not None:
            out = out.map_strings(program.purify.apply)
        return out

    def _eval_atom(self, atom: Atom, *, extract: bool) -> RuleValue:
        if isinstance(atom, ConstAtom):
            return string_value(atom.value) if atom.value else RuleValue.empty()
        if isinstance(atom, JsAtom):
            return self._eval_js(atom)
        if isinstance(atom, PipelineAtom):
            return self._eval_pipeline(atom)
        if isinstance(atom, TemplateAtom):
            expanded = self._expand_template(atom.parts)
            return string_value(expanded) if expanded else RuleValue.empty()
        if isinstance(atom, AllInOneRegexAtom):
            return regex_rows(self.text, atom.pattern)
        if isinstance(atom, JsonPathAtom):
            return self._eval_jsonpath(atom)
        if isinstance(atom, XPathAtom):
            return self._eval_xpath(atom)
        if isinstance(atom, CssAtom):
            return self._eval_css(atom, extract=extract)
        if isinstance(atom, DefaultAtom):
            return self._eval_default(atom, extract=extract)
        raise RuleSyntaxError(f"未知的 atom 类型：{type(atom).__name__}")

    # -------------------------------------------------------------- 各方言

    def _html_items(self):
        """跨 backend 衔接：JSON 字段值是 HTML 片段时自动升格。"""
        if self._value.backend is HTML_BACKEND:
            return self._value.items
        text = self._value.as_string()
        if not text.strip():
            return ()
        return html_value(text).items

    def _eval_default(self, atom: DefaultAtom, *, extract: bool) -> RuleValue:
        # 歧义的单段规则：取值时读作取值关键字，定位时读作定位步。
        # `chapterName = "text"` 和 `bookList = "class.result"` 必须各自拿到对的那一边。
        steps = () if (extract and atom.sole_segment) else atom.steps
        if steps:
            current = RuleValue(backend=HTML_BACKEND, items=tuple(self._html_items()))
        else:
            # 没有定位步骤就直接在当前值上取 —— 保住非 HTML backend。
            # 正则行上的 `$2` 必须走 RegexRowBackend.extract，升格成 HTML 就丢了捕获组。
            current = self._value
        for step in steps:
            if not current.items:
                return RuleValue.empty()
            selected = current.backend.select(current.items, step, self._base_url)
            current = RuleValue(backend=current.backend, items=tuple(selected)).take(step.index)

        if not extract:
            return current
        extractor = atom.extractor or Extractor(kind="text")
        if not current.items:
            return RuleValue.empty()
        strings = current.backend.extract(current.items, extractor, self._base_url)
        return string_value(strings)

    def _eval_css(self, atom: CssAtom, *, extract: bool) -> RuleValue:
        from .nodes import Step

        step = Step(kind="css", name=atom.selector)
        items = self._html_items()
        if not items:
            return RuleValue.empty()
        selected = HTML_BACKEND.select(tuple(items), step, self._base_url)
        current = RuleValue(backend=HTML_BACKEND, items=tuple(selected))
        if not extract or not current.items:
            return current
        extractor = atom.extractor or Extractor(kind="text")
        return string_value(HTML_BACKEND.extract(current.items, extractor, self._base_url))

    def _eval_xpath(self, atom: XPathAtom) -> RuleValue:
        items = self._html_items()
        if not items:
            return RuleValue.empty()
        result = xpath_select(tuple(items), atom.expression)
        return result if result is not None else RuleValue.empty()

    def _eval_jsonpath(self, atom: JsonPathAtom) -> RuleValue:
        if self._value.backend is JSON_BACKEND:
            items = self._value.items
        else:
            text = self.text
            if not text.strip():
                return RuleValue.empty()
            try:
                items = json_value(text).items
            except RuleSyntaxError:
                # 内容不是 JSON 就是「没命中」，不是规则语法错。
                # RuleSyntaxError 留给**规则**本身写错的情况；必填字段为空由
                # `require()` 抛 RuleEmptyError 兜底。
                return RuleValue.empty()
        return jsonpath_select(tuple(items), atom.expression)

    def _eval_pipeline(self, atom: PipelineAtom) -> RuleValue:
        """先求值前半段，结果作为 `result` 喂给 JS。

        phase 1 里 `self._js` 是 `NullJsRuntime`，这里必然抛 `JsNotSupportedError`。
        前半段照样先求值 —— 它可能带 `@put`，副作用要落下；而且 phase 2 接上
        quickjs 后这段代码一行不用改。
        """
        source = self._eval_atom(atom.source, extract=True)
        context = self._js_context()
        context.result = source.as_string() if source.items else ""
        result = self._js.eval(atom.js.script, context)
        if result is None:
            return RuleValue.empty()
        if isinstance(result, (list, tuple)):
            return string_value([str(item) for item in result])
        if isinstance(result, dict):
            return RuleValue(backend=JSON_BACKEND, items=(result,))
        return string_value(str(result))

    def _eval_js(self, atom: JsAtom) -> RuleValue:
        if atom.form == "interpolation":
            resolved = self._expand_interpolation(atom.script)
            if resolved is not None:
                return string_value(resolved)
        result = self._js.eval(atom.script, self._js_context())
        if result is None:
            return RuleValue.empty()
        if isinstance(result, (list, tuple)):
            return string_value([str(item) for item in result])
        if isinstance(result, (dict,)):
            return RuleValue(backend=JSON_BACKEND, items=(result,))
        return string_value(str(result))

    def _expand_interpolation(self, expression: str) -> Optional[str]:
        """`{{key}}` / `{{page}}` / 纯算术不走 JS。"""
        env = {
            "key": self._context.get("key", ""),
            "page": self._context.get("page", 1),
        }
        stripped = expression.strip()
        if stripped == "key":
            return str(env["key"])
        if stripped == "page":
            return str(env["page"])
        computed = MiniExpr.try_eval(stripped, env)
        return None if computed is None else str(computed)

    def _js_context(self) -> JsContext:
        return JsContext(
            base_url=self._base_url,
            result=self._value.as_string() if self._value.items else None,
            variables=self._variables,
            book=self._context.get("book"),
            chapter=self._context.get("chapter"),
            source=self._context.get("source"),
            title=self._context.get("title", ""),
            src=self._context.get("src", ""),
            page=int(self._context.get("page", 1) or 1),
            key=str(self._context.get("key", "")),
        )


def join_url(base_url: str, url: str) -> str:
    """相对 URL 补全。空串、data:、javascript: 原样返回。"""
    url = (url or "").strip()
    if not url:
        return ""
    if url.startswith(("http://", "https://", "data:", "javascript:", "mailto:")):
        return url
    if url.startswith("//"):
        scheme = urlsplit(base_url).scheme or "https"
        return f"{scheme}:{url}"
    if not base_url:
        return url
    return urljoin(base_url, url)


def source_base_url(source_url: str) -> str:
    """`bookSourceUrl` 可能带 `#md5` 或 `#🎃` 后缀，要在 `#` 处截断。"""
    return (source_url or "").split("#", 1)[0].rstrip("/")


def first_non_empty(values: Sequence[str], default: str = "") -> str:
    for value in values:
        if value and value.strip():
            return value
    return default


__all__ = [
    "ContentType",
    "RuleEvaluator",
    "first_non_empty",
    "join_url",
    "source_base_url",
]
