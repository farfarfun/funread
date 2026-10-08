"""规则串 → AST。

切分顺序是硬要求，顺序错了就会被 JS 源码里的 `&&` / `##` 骗到：

1. **遮蔽** `<js>…</js>`、`@js:…`、`{{…}}` → 占位 token
2. 抽取 `@put:{k:rule}`
3. 按 `##` 切 purify
4. 三级 flat split：`&&` → `||` → `%%`
5. 每个 atom 判方言
6. Default atom 内部按 `@` 切 Step

`@get:{k}` 的替换发生在求值期（见 evaluator），不在这里 —— 因为它依赖变量池的当前值。
"""

import re
from functools import lru_cache
from typing import Dict, List, Optional, Tuple

from .errors import RuleSyntaxError
from .nodes import (
    AllInOneRegexAtom,
    Atom,
    ConstAtom,
    CssAtom,
    DefaultAtom,
    Extractor,
    IndexSpec,
    JsAtom,
    JsonPathAtom,
    PipelineAtom,
    PurifySpec,
    RuleProgram,
    Step,
    TemplateAtom,
    XPathAtom,
)

# 遮蔽用的哨兵。\x00 不可能出现在真实规则串里。
_MASK_PREFIX = "\x00J"
_MASK_SUFFIX = "\x00"
_MASK_RE = re.compile(r"\x00J(\d+)\x00")

_TAG_JS_RE = re.compile(r"<js>(.*?)</js>", re.S | re.I)
_AT_JS_RE = re.compile(r"@js:(.*)", re.S)
_INTERPOLATION_RE = re.compile(r"\{\{(.*?)\}\}", re.S)
_PUT_RE = re.compile(r"@put:\{([^}]*)\}")
_GET_RE = re.compile(r"@get:\{([^}]*)\}")

_EXTRACTOR_KEYWORDS = {
    "text": "text",
    "textnodes": "textNodes",
    "ownText": "ownText",
    "owntext": "ownText",
    "html": "html",
    "innerhtml": "html",
    "outerhtml": "outerHtml",
    "all": "all",
}

# 尾部索引。`[...]` 的内容必须长得像索引（数字/逗号/冒号/!/负号），否则就是 CSS
# 属性选择器 —— `[property="og:description"]` 不能被当成切片。
#
# `!` 排除**不带前导点**也成立：Legado 的 `ElementsSingle` 先按 `!` 把排除项切下来，
# 再对剩下的部分做类型派发。所以 `tr!0` = CSS 选择器 `tr` 去掉第 0 个，少了这一支
# 整个 `tr!0` 会原样丢给 cssselect 并抛 RuleSyntaxError。
_INDEX_RE = re.compile(
    r"^(?P<body>.*?)(?P<index>\[[\s\d,:!+-]*\]|\.!-?\d+|\.-?\d+|!-?\d+(?:\s*,\s*-?\d+)*)?$",
    re.S,
)


class MaskTable:
    """遮蔽期间记录被挖掉的 JS 片段。"""

    def __init__(self) -> None:
        self._items: List[JsAtom] = []

    def add(self, atom: JsAtom) -> str:
        self._items.append(atom)
        return f"{_MASK_PREFIX}{len(self._items) - 1}{_MASK_SUFFIX}"

    def get(self, token: int) -> JsAtom:
        return self._items[token]

    @property
    def items(self) -> Tuple[JsAtom, ...]:
        return tuple(self._items)

    def __bool__(self) -> bool:
        return bool(self._items)


def mask_js(rule: str) -> Tuple[str, MaskTable]:
    """把所有 JS 片段替换成占位 token。必须在任何 split 之前调用。"""
    table = MaskTable()

    def tag_repl(match: re.Match) -> str:
        return table.add(JsAtom(script=match.group(1), form="tag_js", raw=match.group(0)))

    def interp_repl(match: re.Match) -> str:
        return table.add(JsAtom(script=match.group(1), form="interpolation", raw=match.group(0)))

    masked = _TAG_JS_RE.sub(tag_repl, rule)
    masked = _INTERPOLATION_RE.sub(interp_repl, masked)

    # `@js:` 只能在尾部，后面的所有内容都是脚本。
    at_js = _AT_JS_RE.search(masked)
    if at_js:
        token = table.add(JsAtom(script=at_js.group(1), form="at_js", raw=at_js.group(0)))
        masked = masked[: at_js.start()] + token
    return masked, table


def unmask(text: str, table: MaskTable) -> str:
    """把占位 token 还原成原始片段（用于错误信息与非 JS 场景的回显）。"""

    def repl(match: re.Match) -> str:
        return table.get(int(match.group(1))).raw

    return _MASK_RE.sub(repl, text)


_TEMPLATE_PIECE_RE = re.compile(r"<js>(?P<tag>.*?)</js>|\{\{(?P<interp>.*?)\}\}", re.S | re.I)


def split_template(text: str) -> Tuple[Tuple[str, str], ...]:
    """把字符串模板切成 `("lit", 文本)` / `("js", 脚本)` 序列。

    URL 字段（`searchUrl`/`exploreUrl`/…）走的是**字符串插值**，不是选择器求值 ——
    Legado 里这两条路径是分开的，`/search?q={{key}}` 必须原样保留 `/search?q=` 前缀。
    """
    parts: List[Tuple[str, str]] = []
    pos = 0
    for match in _TEMPLATE_PIECE_RE.finditer(text):
        if match.start() > pos:
            parts.append(("lit", text[pos : match.start()]))
        script = match.group("tag")
        if script is None:
            script = match.group("interp") or ""
        parts.append(("js", script))
        pos = match.end()
    if pos < len(text):
        parts.append(("lit", text[pos:]))
    return tuple(parts)


def has_template_placeholder(text: str) -> bool:
    return bool(_TEMPLATE_PIECE_RE.search(text or ""))


def has_get_reference(rule: str) -> bool:
    return "@get:" in rule


def substitute_get(rule: str, lookup) -> str:
    """把 `@get:{k}` 替换成变量池里的当前值。"""
    return _GET_RE.sub(lambda m: lookup(m.group(1).strip()), rule)


def _extract_puts(rule: str) -> Tuple[str, Tuple[Tuple[str, str], ...]]:
    puts: List[Tuple[str, str]] = []

    def repl(match: re.Match) -> str:
        body = match.group(1)
        key, _, sub = body.partition(":")
        puts.append((key.strip(), sub.strip()))
        return ""

    stripped = _PUT_RE.sub(repl, rule)
    return stripped, tuple(puts)


def _split_purify(rule: str) -> Tuple[str, Optional[PurifySpec]]:
    """切出 `##regex##replacement[###]`。

    注意 `##` 可能出现在 URL（`https://x/#/a`）里，所以只在**出现至少一次 `##`**
    且左侧非空时才认。`###` 结尾表示只替换第一个。
    """
    if "##" not in rule:
        return rule, None
    body, _, tail = rule.partition("##")
    if not tail:
        return rule, None

    first_only = False
    if tail.endswith("###"):
        first_only = True
        tail = tail[:-3]
    elif tail.endswith("##"):
        tail = tail[:-2]

    pattern_src, _, replacement = tail.partition("##")
    try:
        pattern = re.compile(pattern_src)
    except re.error as exc:
        raise RuleSyntaxError(f"purify 正则无法编译：{pattern_src!r}", rule=rule) from exc
    return body, PurifySpec(
        pattern=pattern,
        replacement=_convert_dollar_refs(replacement),
        first_only=first_only,
    )


_DOLLAR_REF_RE = re.compile(r"\$(\d{1,2})")
_GROUP_ONLY_RE = re.compile(r"\$\d{1,2}")


def _convert_dollar_refs(replacement: str) -> str:
    """Legado 用 `$1` 引用捕获组，Python re 要 `\\1`。"""
    return _DOLLAR_REF_RE.sub(lambda m: "\\" + m.group(1), replacement.replace("\\", "\\\\"))


def _flat_split(text: str, sep: str) -> List[str]:
    return [part for part in text.split(sep)]


def _parse_index(spec: str) -> Optional[IndexSpec]:
    spec = spec.strip()
    if not spec:
        return None
    if spec.startswith("!"):
        #  不带点的排除式：`tr!0`、`tag.li!0,1`
        return IndexSpec(
            kind="exclude",
            values=tuple(int(bit) for bit in spec[1:].split(",") if bit.strip()),
        )
    if spec.startswith("."):
        raw = spec[1:]
        if raw.startswith("!"):
            return IndexSpec(kind="exclude", values=(int(raw[1:]),))
        return IndexSpec(kind="single", values=(int(raw),))

    inner = spec[1:-1].strip()
    if not inner:
        return None
    if ":" in inner:
        bits = inner.split(":")
        if len(bits) > 3:
            raise RuleSyntaxError(f"切片语法不合法：{spec!r}")
        start, stop, step = (bits + ["", "", ""])[:3]
        return IndexSpec(
            kind="slice",
            start=int(start) if start.strip() else None,
            stop=int(stop) if stop.strip() else None,
            step=int(step) if step.strip() else None,
        )

    values = [bit.strip() for bit in inner.split(",") if bit.strip()]
    if not values:
        return None
    excludes = [v for v in values if v.startswith("!")]
    if excludes:
        if len(excludes) != len(values):
            raise RuleSyntaxError(f"索引里不能混用 ! 与普通下标：{spec!r}")
        return IndexSpec(kind="exclude", values=tuple(int(v[1:]) for v in values))
    return IndexSpec(kind="list", values=tuple(int(v) for v in values))


def _looks_like_css(segment: str) -> bool:
    return bool(re.search(r"[\[\]:>~#.]", segment)) or " " in segment.strip()


def _parse_step(segment: str) -> Step:
    """解析一个 Default 定位步骤。"""
    match = _INDEX_RE.match(segment)
    body = match.group("body") if match else segment
    index = _parse_index(match.group("index") or "") if match else None

    body = body.strip()
    if not body:
        return Step(kind="index", index=index)

    if body.isdigit() or (body.startswith("-") and body[1:].isdigit()):
        # 裸数字等价于 children[n]
        return Step(kind="children", index=IndexSpec(kind="single", values=(int(body),)))

    kind, sep, name = body.partition(".")
    kind_lower = kind.strip().lower()
    if sep and kind_lower in {"class", "id", "tag", "text"}:
        # 名字为空也保留这个 kind（backend 会返回空）。`id.` 是 `@get:{}` 没取到值时
        # 的残留形态，当成 CSS 选择器丢给 cssselect 会抛 RuleSyntaxError ——
        # 选择器落空必须是「空结果」，不能炸掉整条流程。
        return Step(kind=kind_lower, name=name.strip(), index=index)  # type: ignore[arg-type]
    if kind_lower == "children" and not name:
        return Step(kind="children", index=index)
    #  认不出类型就猜它是 CSS 选择器 —— 猜错了算没命中，不能炸掉整条流程
    return Step(kind="css", name=body, index=index, lenient=True)


def _as_extractor(segment: str) -> Optional[Extractor]:
    """最后一段是否是取值关键字。不是就当定位步，由调用方兜底成 text。"""
    seg = segment.strip()
    if not seg:
        return None
    low = seg.lower()
    if low in _EXTRACTOR_KEYWORDS:
        return Extractor(kind=_EXTRACTOR_KEYWORDS[low])  # type: ignore[arg-type]
    if seg in _EXTRACTOR_KEYWORDS:
        return Extractor(kind=_EXTRACTOR_KEYWORDS[seg])  # type: ignore[arg-type]
    if re.fullmatch(r"\$\d{1,2}", seg):
        return Extractor(kind="group", name=seg[1:])
    # 单个标识符且不含定位语法 → 当属性名
    if re.fullmatch(r"[A-Za-z_][\w:.-]*", seg) and "." not in seg:
        return Extractor(kind="attr", name=seg)
    return None


def _parse_default_atom(segment: str) -> DefaultAtom:
    raw = segment
    if segment.startswith("@@"):
        segment = segment[2:]
    parts = [p for p in segment.split("@")]
    if not parts:
        return DefaultAtom(steps=(), extractor=None, raw=raw)

    if len(parts) > 1:
        extractor = _as_extractor(parts[-1])
        step_parts = parts[:-1] if extractor is not None else parts
        sole = False
    else:
        # 只有一段：`text` / `href` 这种既像定位步又像取值关键字。两种解释都留着，
        # 由 `_eval_default` 按 extract 开关挑 —— 列表规则（`class.result`）要定位，
        # 字段规则（`chapterName = "text"`）要取值。
        extractor = _as_extractor(parts[0])
        step_parts = parts
        sole = extractor is not None

    # 空分段直接丢掉：前导 `@`（`@name`）只是个分隔符，不该变成一个定位步骤。
    # 留着它会让 steps 非空，进而把当前值强行升格成 HTML —— JSON 行上的 `@name` 就取不到了。
    steps = tuple(_parse_step(p) for p in step_parts if p.strip())
    return DefaultAtom(steps=steps, extractor=extractor, raw=raw, sole_segment=sole)


def _parse_css_atom(segment: str) -> CssAtom:
    body = segment[len("@css:") :]
    parts = body.split("@")
    extractor = _as_extractor(parts[-1]) if len(parts) > 1 else None
    selector = "@".join(parts[:-1]) if extractor is not None else body
    return CssAtom(selector=selector.strip(), extractor=extractor, raw=segment)


def _find_pipeline_token(segment: str, table: MaskTable):
    """找第一个管道形态（`at_js`/`tag_js`）的遮蔽 token。"""
    for match in _MASK_RE.finditer(segment):
        atom = table.get(int(match.group(1)))
        if atom.form in ("at_js", "tag_js"):
            return match, atom
    return None


def _parse_atom(segment: str, table: MaskTable) -> Atom:
    stripped = segment.strip()
    if not stripped:
        return ConstAtom(value="")

    mask_match = _MASK_RE.fullmatch(stripped)
    if mask_match:
        return table.get(int(mask_match.group(1)))

    # 含 token 但不只有 token。两种形态必须分开处理：
    #   - 管道形态（`@js:` / `<js></js>`）：`id.x@text@js:result` —— 前缀是规则，
    #     JS 吃它的结果。只能跑 JS 或显式报错，不能当字面量。
    #   - 插值形态（`{{}}`）：`/s?q={{key}}&p={{page}}` —— 按片展开，纯占位不碰 JS。
    if _MASK_RE.search(stripped):
        pipeline = _find_pipeline_token(stripped, table)
        if pipeline is not None:
            match, js_atom = pipeline
            prefix = stripped[: match.start()].strip()
            if not prefix:
                return js_atom
            return PipelineAtom(source=_parse_atom(prefix, table), js=js_atom, raw=stripped)
        restored = unmask(stripped, table)
        return TemplateAtom(parts=split_template(restored), raw=stripped)

    if stripped.startswith("@css:"):
        return _parse_css_atom(stripped)
    if stripped.startswith("@xpath:"):
        return XPathAtom(expression=stripped[len("@xpath:") :].strip(), raw=stripped)
    if stripped.startswith("//"):
        return XPathAtom(expression=stripped, raw=stripped)
    if stripped.startswith("@json:"):
        return JsonPathAtom(expression=stripped[len("@json:") :].strip(), raw=stripped)
    if stripped.startswith("$.") or stripped.startswith("$["):
        return JsonPathAtom(expression=stripped, raw=stripped)
    if _GROUP_ONLY_RE.fullmatch(stripped):
        # 单独一个 `$2`：AllInOne 正则行里的捕获组引用，没有定位步骤。
        # 不能走 _parse_default_atom —— 那里只在 `@` 分段数 >1 时才认取值关键字，
        # `$2` 会被当成 CSS 选择器。
        return DefaultAtom(
            steps=(), extractor=Extractor(kind="group", name=stripped[1:]), raw=stripped
        )
    if stripped.startswith(":"):
        try:
            pattern = re.compile(stripped[1:], re.S)
        except re.error as exc:
            raise RuleSyntaxError(f"AllInOne 正则无法编译：{stripped[1:]!r}", rule=segment) from exc
        return AllInOneRegexAtom(pattern=pattern, raw=stripped)
    return _parse_default_atom(stripped)


def parse_rule(rule: Optional[str]) -> RuleProgram:
    """编译一条规则。含 `@get:` 的串不走缓存（它依赖运行期变量值）。"""
    if rule is None:
        return RuleProgram(groups=(), raw="")
    if has_get_reference(rule):
        return _parse_rule_uncached(rule)
    return _parse_rule_cached(rule)


@lru_cache(maxsize=8192)
def _parse_rule_cached(rule: str) -> RuleProgram:
    return _parse_rule_uncached(rule)


def _parse_rule_uncached(rule: str) -> RuleProgram:
    if not rule or not rule.strip():
        return RuleProgram(groups=(), raw=rule or "")

    masked, table = mask_js(rule)
    masked, put_specs = _extract_puts(masked)
    masked, purify = _split_purify(masked)

    puts = tuple((key, parse_rule(unmask(sub, table))) for key, sub in put_specs if key)

    if not masked.strip():
        return RuleProgram(groups=(), purify=purify, puts=puts, raw=rule)

    groups: List[Tuple[Tuple[Atom, ...], ...]] = []
    for and_part in _flat_split(masked, "&&"):
        or_items: List[Tuple[Atom, ...]] = []
        for or_part in _flat_split(and_part, "||"):
            interleaved = tuple(_parse_atom(piece, table) for piece in _flat_split(or_part, "%%"))
            if interleaved:
                or_items.append(interleaved)
        if or_items:
            groups.append(tuple(or_items))

    return RuleProgram(groups=tuple(groups), purify=purify, puts=puts, raw=rule)


def parse_replace_regex(rule: Optional[str]) -> Tuple[PurifySpec, ...]:
    """解析 `replaceRegex`：多条用换行分隔，每条 `##pattern##replacement`。"""
    if not rule or not rule.strip():
        return ()
    specs: List[PurifySpec] = []
    for line in (ln.strip() for ln in rule.splitlines()):
        if not line:
            continue
        body = line[2:] if line.startswith("##") else line
        first_only = False
        if body.endswith("###"):
            first_only = True
            body = body[:-3]
        pattern_src, _, replacement = body.partition("##")
        if not pattern_src:
            continue
        try:
            compiled = re.compile(pattern_src)
        except re.error:
            continue
        specs.append(
            PurifySpec(
                pattern=compiled,
                replacement=_convert_dollar_refs(replacement),
                first_only=first_only,
            )
        )
    return tuple(specs)


def parse_named_urls(raw: Optional[str]) -> List[Tuple[str, str]]:
    """解析 `exploreUrl` / `sortUrl` 的多行 `名称::URL` 形式。"""
    if not raw or not raw.strip():
        return []
    out: List[Tuple[str, str]] = []
    for line in (ln.strip() for ln in raw.splitlines()):
        if not line:
            continue
        name, sep, url = line.partition("::")
        if sep:
            out.append((name.strip(), url.strip()))
        else:
            out.append(("", line))
    return out


def scan_features(rule_strings) -> Dict[str, bool]:
    """静态体检：这批规则串里有没有 JS / webView。零网络、零求值。"""
    needs_js = False
    for rule in rule_strings:
        if not rule or not isinstance(rule, str):
            continue
        if _TAG_JS_RE.search(rule) or _AT_JS_RE.search(rule):
            needs_js = True
            continue
        for match in _INTERPOLATION_RE.finditer(rule):
            if not _is_placeholder_expression(match.group(1)):
                needs_js = True
                break
    return {"needs_js": needs_js}


_PLACEHOLDER_RE = re.compile(r"^[\s\d()+\-*/%.]*(?:\b(?:key|page)\b[\s\d()+\-*/%.]*)*$")


def _is_placeholder_expression(expr: str) -> bool:
    """`{{key}}` / `{{page}}` / `{{(page-1)*20}}` 这类不需要 JS。"""
    expr = expr.strip()
    if not expr:
        return False
    if re.search(r"[A-Za-z_]\w*", expr):
        names = set(re.findall(r"[A-Za-z_]\w*", expr))
        if not names <= {"key", "page"}:
            return False
    return bool(_PLACEHOLDER_RE.match(expr))


__all__ = [
    "MaskTable",
    "has_get_reference",
    "has_template_placeholder",
    "mask_js",
    "parse_named_urls",
    "parse_replace_regex",
    "parse_rule",
    "scan_features",
    "split_template",
    "substitute_get",
    "unmask",
]
