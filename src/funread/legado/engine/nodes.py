"""规则 AST。

AST 里 JS 节点（`JsAtom`）和各方言节点是**同级**的：`@js:` 是规则管道的一环，
`{{}}` 是字符串插值的一环。第一版就把位置留好，phase 2 接 quickjs 时解析器一行不改。
"""

import re
from dataclasses import dataclass, field
from typing import Literal, Optional, Tuple, Union

IndexKind = Literal["single", "list", "slice", "exclude"]
StepKind = Literal["class", "id", "tag", "text", "children", "css", "index"]
ExtractorKind = Literal[
    "text",
    "textNodes",
    "ownText",
    "html",
    "outerHtml",
    "all",
    "attr",
    "group",
]


@dataclass(frozen=True)
class IndexSpec:
    """Default 方言的索引/切片/排除语法。"""

    kind: IndexKind
    values: Tuple[int, ...] = ()
    start: Optional[int] = None
    stop: Optional[int] = None
    step: Optional[int] = None

    def apply(self, items: Tuple) -> Tuple:
        total = len(items)
        if self.kind == "slice":
            return items[slice(self.start, self.stop, self.step)]

        def resolve(i: int) -> int:
            return i if i >= 0 else total + i

        if self.kind == "exclude":
            drop = {resolve(i) for i in self.values}
            return tuple(item for pos, item in enumerate(items) if pos not in drop)

        picked = []
        for raw in self.values:
            pos = resolve(raw)
            if 0 <= pos < total:
                picked.append(items[pos])
        return tuple(picked)


@dataclass(frozen=True)
class Step:
    """Default 方言的一个定位步骤。"""

    kind: StepKind
    name: str = ""
    index: Optional[IndexSpec] = None
    #  `lenient` 只给 Default 方言「猜是 CSS」的兜底分支用：那一段本来可能是别的东西，
    #  猜错了就该是「没命中」。`@css:` 是作者明确写的选择器，翻译不了就得报错。
    lenient: bool = False


@dataclass(frozen=True)
class Extractor:
    """Default 方言最后一段的取值方式。"""

    kind: ExtractorKind
    name: str = ""


@dataclass(frozen=True)
class DefaultAtom:
    """Default(JSOUP) 方言：`type.name.index@…@output`。

    `sole_segment` 标记「整条规则只有一段、且这一段既可读作定位步也可读作取值
    关键字」的歧义情形 —— 典型就是 `ruleToc.chapterName = "text"` 与
    `chapterUrl = "href"`。Legado 对这种规则有**两个入口**：列表规则走
    `getElements`（每段都是定位），字段规则走 `getString`（最后一段是取值）。
    语法上分不开，所以这里两种解释都留着，由求值时的 `extract` 开关来选 ——
    见 `evaluator._eval_default`。
    """

    steps: Tuple[Step, ...]
    extractor: Optional[Extractor]
    raw: str
    sole_segment: bool = False


@dataclass(frozen=True)
class CssAtom:
    selector: str
    extractor: Optional[Extractor]
    raw: str


@dataclass(frozen=True)
class XPathAtom:
    expression: str
    raw: str


@dataclass(frozen=True)
class JsonPathAtom:
    expression: str
    raw: str


@dataclass(frozen=True)
class AllInOneRegexAtom:
    """前导 `:` 的 AllInOne 正则，整页上跑 finditer，每个 match 是一行。"""

    pattern: re.Pattern
    raw: str


@dataclass(frozen=True)
class JsAtom:
    """`@js:` / `<js></js>` / 非占位 `{{}}`。"""

    script: str
    form: Literal["at_js", "tag_js", "interpolation"]
    raw: str


@dataclass(frozen=True)
class ConstAtom:
    """字面量（`@get` 替换后的产物，或模板展开后的纯字符串）。"""

    value: str


@dataclass(frozen=True)
class TemplateAtom:
    """字面量与 `{{}}` 占位符混排，例如 `/search?q={{key}}&p={{page}}`。

    不能把整串丢给 JS —— 那样 `{{key}}` 这种纯占位也会要求 JS 引擎。按片展开，
    只有真正是 JS 的片才会落到 runtime 上。

    只装 `{{}}`（字符串插值形态）。`@js:` / `<js></js>` 是**管道**形态，见 `PipelineAtom`。
    """

    parts: Tuple[Tuple[Literal["lit", "js"], str], ...]
    raw: str


@dataclass(frozen=True)
class PipelineAtom:
    """`<选择器>@js:<脚本>`：先求值 source，结果作为 `result` 喂给脚本。

    和 `TemplateAtom` 必须分开。曾经把它也当字符串插值处理，结果
    `id.x@text@js:result` 里没有 `{{}}` 可展开，整条规则被当成字面量原样返回 ——
    比降级更糟（拿到的是规则串本身）。管道形态只有两种结局：跑 JS，或者显式报错。
    """

    source: "Atom"
    js: "JsAtom"
    raw: str


Atom = Union[
    DefaultAtom,
    CssAtom,
    XPathAtom,
    JsonPathAtom,
    AllInOneRegexAtom,
    JsAtom,
    ConstAtom,
    TemplateAtom,
    PipelineAtom,
]


@dataclass(frozen=True)
class PurifySpec:
    """`##regex##replacement[###]` 的净化规则，作用在字符串层。"""

    pattern: re.Pattern
    replacement: str
    first_only: bool = False

    def apply(self, text: str) -> str:
        count = 1 if self.first_only else 0
        try:
            return self.pattern.sub(self.replacement, text, count=count)
        except re.error:
            return text


@dataclass(frozen=True)
class RuleProgram:
    """一条完整规则。

    `groups` 是三级组合符的嵌套结果：最外层 `&&`，中层 `||`，内层 `%%`。
    """

    groups: Tuple[Tuple[Tuple[Atom, ...], ...], ...]
    purify: Optional[PurifySpec] = None
    puts: Tuple[Tuple[str, "RuleProgram"], ...] = field(default_factory=tuple)
    raw: str = ""

    @property
    def is_empty(self) -> bool:
        return not self.groups


__all__ = [
    "AllInOneRegexAtom",
    "Atom",
    "ConstAtom",
    "CssAtom",
    "DefaultAtom",
    "Extractor",
    "IndexSpec",
    "JsAtom",
    "JsonPathAtom",
    "PipelineAtom",
    "PurifySpec",
    "RuleProgram",
    "Step",
    "TemplateAtom",
    "XPathAtom",
]
