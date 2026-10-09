"""`SourceSpec`：把脏源 dict 洗成规范结构。

引擎其余部分只看 `SourceSpec`，所有格式漂移都收在这一个文件里。要吸收的坑分三类：

1. **Legado 自己的版本演进** —— 2.x 的扁平字段名（`ruleSearchList`）与 3.x 的
   嵌套结构（`ruleSearch.bookList`）并存，同一个字段还有多个历史别名。
2. **类型漂移** —— `bookSourceType` 可能是 int 也可能是 str；`header` 可能是
   dict 也可能是 JSON 字符串；`ruleContent` 偶尔整体是一条字符串规则。
3. **本仓库采集阶段留下的伤**（`manage/download/sources/book.py`）——
   - `bookSourceComment` 被清空（有些源在注释里放 JS 库），phase 1 用不上，记录即可。
   - `bookSourceUrl` 被 `rstrip("/|#")`。
   - `searchUrl`/`exploreUrl` 以及 `ruleSearch.url`/`ruleToc.url`/`ruleContent.url`
     里的 base_url 被 `str.replace` 剪掉了 → **必须重新拼回去**，否则发不出请求。
   - 空值字段被整个删掉 → 取规则一律用 `get`，不能假设 key 存在。
   - `ruleBookContent` 被映射到 `ruleBookInfo.content`，但它在 Legado 里是**章节正文**
     规则。这里按 Legado 语义纠正回 `ruleContent.content`。
"""

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Tuple

from .evaluator import source_base_url
from .fetch import split_url_options
from .lexer import parse_named_urls, scan_features
from .models import ExploreKind

# 包装字段：采集/合并流程塞进来的元数据，不是源规则。
_WRAPPER_KEYS = frozenset(
    {
        "available",
        "candidate",
        "final",
        "merged",
        "url_id",
        "hostname",
        "status",
        "_id",
        "id",
        "md5",
        "source_type",
        "cate1",
        "cate2",
    }
)

# 顶层旧别名 → 新名。
_TOP_ALIASES = {
    "searchRule": "ruleSearch",
    "bookInfoRule": "ruleBookInfo",
    "tocRule": "ruleToc",
    "catalogRule": "ruleToc",
    "contentRule": "ruleContent",
    "exploreRule": "ruleExplore",
    "findRule": "ruleExplore",
    "httpUserAgent": "header",
    "ruleFindUrl": "exploreUrl",
}

# 组内别名 → 规范名。`noteUrl` 是 2.x 里 bookUrl 的名字，必须认。
_GROUP_ALIASES: Dict[str, Dict[str, str]] = {
    "ruleSearch": {
        "noteUrl": "bookUrl",
        "list": "bookList",
        "introduce": "intro",
        "tocUrl": "bookUrl",
    },
    "ruleExplore": {
        "noteUrl": "bookUrl",
        "list": "bookList",
        "introduce": "intro",
    },
    "ruleBookInfo": {
        "introduce": "intro",
        "noteUrl": "bookUrl",
    },
    "ruleToc": {
        "list": "chapterList",
        "chapterUrlNext": "nextTocUrl",
        "urlNext": "nextTocUrl",
        "nextUrl": "nextTocUrl",
    },
    "ruleContent": {
        "urlNext": "nextContentUrl",
        "nextUrl": "nextContentUrl",
        "contentUrlNext": "nextContentUrl",
    },
}

_RULE_GROUPS = ("ruleSearch", "ruleExplore", "ruleBookInfo", "ruleToc", "ruleContent")

#: 订阅源的组名。只有一个 —— RSS 源的规则在 Legado 里全是扁平字段，没有嵌套结构。
_RSS_RULE_GROUPS = ("ruleRss",)

#: `ruleRss: {...}` 嵌套形态里的别名 → 规范名。
_RSS_GROUP_ALIASES: Dict[str, str] = {
    "list": "articles",
    "articleList": "articles",
    "nextArticles": "nextPage",
    "nextUrl": "nextPage",
    "urlNext": "nextPage",
    "date": "pubDate",
    "describe": "description",
    "introduce": "description",
}

# Legado 2.x 的扁平字段名 → (组, 规范名)。
# 与采集侧 `manage/download/sources/book.py` 的 `__format_base` 一致，只有一处故意不同：
# `ruleBookContent` 直接进 `ruleContent.content` —— 它是**章节正文**规则，
# 采集侧把它塞进了 `ruleBookInfo.content`，那是错的（见 `_repair_content_rule`）。
_FLAT_TO_GROUP: Dict[str, Tuple[str, str]] = {
    # 搜索
    "ruleSearchList": ("ruleSearch", "bookList"),
    "ruleSearchName": ("ruleSearch", "name"),
    "ruleSearchAuthor": ("ruleSearch", "author"),
    "ruleSearchNoteUrl": ("ruleSearch", "bookUrl"),
    "ruleSearchCoverUrl": ("ruleSearch", "coverUrl"),
    "ruleSearchIntroduce": ("ruleSearch", "intro"),
    "ruleSearchKind": ("ruleSearch", "kind"),
    "ruleSearchLastChapter": ("ruleSearch", "lastChapter"),
    "ruleSearchWordCount": ("ruleSearch", "wordCount"),
    # 发现
    "ruleFindList": ("ruleExplore", "bookList"),
    "ruleFindName": ("ruleExplore", "name"),
    "ruleFindAuthor": ("ruleExplore", "author"),
    "ruleFindNoteUrl": ("ruleExplore", "bookUrl"),
    "ruleFindCoverUrl": ("ruleExplore", "coverUrl"),
    "ruleFindIntroduce": ("ruleExplore", "intro"),
    "ruleFindKind": ("ruleExplore", "kind"),
    "ruleFindLastChapter": ("ruleExplore", "lastChapter"),
    # 详情
    "ruleBookName": ("ruleBookInfo", "name"),
    "ruleBookAuthor": ("ruleBookInfo", "author"),
    "ruleBookKind": ("ruleBookInfo", "kind"),
    "ruleBookLastChapter": ("ruleBookInfo", "lastChapter"),
    "ruleBookWordCount": ("ruleBookInfo", "wordCount"),
    "ruleIntroduce": ("ruleBookInfo", "intro"),
    "ruleBookInfoInit": ("ruleBookInfo", "init"),
    "ruleBookUrlPattern": ("ruleBookInfo", "urlPattern"),
    "ruleCoverUrl": ("ruleBookInfo", "coverUrl"),
    # 目录
    "ruleChapterList": ("ruleToc", "chapterList"),
    "ruleChapterName": ("ruleToc", "chapterName"),
    "ruleChapterUrl": ("ruleToc", "chapterUrl"),
    "ruleChapterUrlNext": ("ruleToc", "nextTocUrl"),
    "ruleChapterUpdateTime": ("ruleToc", "updateTime"),
    # 正文
    "ruleBookContent": ("ruleContent", "content"),
    "ruleContentUrl": ("ruleContent", "nextContentUrl"),
    "ruleContentUrlNext": ("ruleContent", "nextContentUrl"),
    "ruleBookContentReplaceRegex": ("ruleContent", "replaceRegex"),
    "ruleBookContentReplace": ("ruleContent", "replaceRegex"),
    "ruleBookContentSourceRegex": ("ruleContent", "sourceRegex"),
    "ruleBookContentWebJs": ("ruleContent", "webJs"),
}

# 八个核心链路字段。缺任意一个这个源就走不完「搜索→正文」。
CORE_FIELDS: Tuple[Tuple[str, ...], ...] = (
    ("searchUrl",),
    ("ruleSearch", "bookList"),
    ("ruleSearch", "name"),
    ("ruleSearch", "bookUrl"),
    ("ruleToc", "chapterList"),
    ("ruleToc", "chapterName"),
    ("ruleToc", "chapterUrl"),
    ("ruleContent", "content"),
)

# 订阅源的扁平字段名 → (组, 规范名)。
#
# `ruleNextPage` 与 `ruleNextArticles` 归一到同一个键：前者是归档里真正在用的名字
# （1,344 个源里 449 个 / 33.4%），后者是我们自家 `manage/publish/rss.py` 产出的源
# 在用。两个都要认，`_collect_flat_rules` 按本表的声明顺序定优先级，所以把
# `ruleNextPage` 放在前面。
#
# `ruleContent` 在这里进 `ruleRss.content` 而不是书源的 `ruleContent` 组 —— 两套
# 归一化按 `source_type` 完全分派，一个 RSS 源不该冒出 `ruleToc`，反之亦然。
_RSS_FLAT_TO_GROUP: Dict[str, Tuple[str, str]] = {
    "ruleArticles": ("ruleRss", "articles"),
    "ruleTitle": ("ruleRss", "title"),
    "ruleLink": ("ruleRss", "link"),
    "rulePubDate": ("ruleRss", "pubDate"),
    "ruleImage": ("ruleRss", "image"),
    "ruleDescription": ("ruleRss", "description"),
    "ruleContent": ("ruleRss", "content"),
    "ruleNextPage": ("ruleRss", "nextPage"),
    "ruleNextArticles": ("ruleRss", "nextPage"),
}

# 订阅源的核心链路字段，只有三个。
#
# 书源要八个是因为它有「搜索→详情→目录→正文」四段；RSS 只有「列表→（可选）正文」，
# 拿到一个能遍历的列表和每项的标题就已经能用了。`ruleLink` 实测只有 40.3%，但缺了
# 可以拿列表项自身的 href 兜底（见 `RssSourceEngine._link_of`），所以不进核心集 ——
# 把它算进去会把可用源从 10.8% 再砍掉一截，而那部分其实是能跑的。
RSS_CORE_FIELDS: Tuple[Tuple[str, ...], ...] = (
    ("sourceUrl",),
    ("ruleRss", "articles"),
    ("ruleRss", "title"),
)


def _as_text(value: Any) -> str:
    """规则值统一成字符串。源里偶见 list（多条候选）/ int。"""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else ""
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (list, tuple)):
        # 多条候选规则等价于 `||`：取第一个非空
        parts = [_as_text(item) for item in value]
        return "||".join(p for p in parts if p.strip())
    if isinstance(value, Mapping):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() not in {"", "0", "false", "no", "null", "none"}


def _parse_header(value: Any) -> Dict[str, str]:
    """`header` 可能是 dict、JSON 字符串，也可能干脆就是一个裸 UA 字符串。"""
    if not value:
        return {}
    if isinstance(value, Mapping):
        return {str(k): str(v) for k, v in value.items()}
    text = str(value).strip()
    if not text:
        return {}
    if text.startswith("{"):
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return {"User-Agent": text}
        if isinstance(parsed, Mapping):
            return {str(k): str(v) for k, v in parsed.items()}
        return {}
    # `httpUserAgent` 迁移过来的裸 UA
    return {"User-Agent": text}


@dataclass
class SourceSpec:
    """归一化后的源。`rule(group, name)` 是取规则的唯一入口。"""

    raw: Dict[str, Any] = field(default_factory=dict)
    source_type: str = "book"
    url: str = ""
    name: str = ""
    group: str = ""
    comment: str = ""
    login_url: str = ""
    header: Dict[str, str] = field(default_factory=dict)
    search_url: str = ""
    explore_url: str = ""
    book_source_type: int = 0
    respond_time: int = 0
    enabled: bool = True
    #: 订阅源的图标地址（`sourceIcon`，48.4% 的源有）。
    icon: str = ""
    #: 订阅源的多分类入口，多行 `名称::URL`（`sortUrl`，32.4%）。
    sort_url: str = ""
    #: 非空表示这是个 WebView 型源（`singleUrl`，21.9%）—— 纯 Python 跑不了。
    single_url: str = ""
    #: 相对链接要不要按页面地址绝对化（`loadWithBaseUrl`，94.4% 开着）。
    load_with_base_url: bool = False
    variables: Dict[str, str] = field(default_factory=dict)
    _groups: Dict[str, Dict[str, str]] = field(default_factory=dict)

    # ------------------------------------------------------------------ 构造

    @classmethod
    def from_dict(cls, data: Mapping[str, Any], *, source_type: str = "book") -> "SourceSpec":
        payload = _unwrap(data)
        normalized = _normalize_top(payload)
        groups = _normalize_groups(normalized, source_type)
        if source_type != "rss":
            _repair_content_rule(groups)

        url = source_base_url(
            _as_text(normalized.get("bookSourceUrl") or normalized.get("sourceUrl"))
        )
        spec = cls(
            raw=dict(payload),
            source_type=source_type,
            url=url,
            name=_as_text(normalized.get("bookSourceName") or normalized.get("sourceName")),
            group=_as_text(normalized.get("bookSourceGroup") or normalized.get("sourceGroup")),
            comment=_as_text(
                normalized.get("bookSourceComment") or normalized.get("sourceComment")
            ),
            login_url=_as_text(normalized.get("loginUrl")),
            header=_parse_header(normalized.get("header")),
            search_url=_as_text(normalized.get("searchUrl")),
            explore_url=_as_text(normalized.get("exploreUrl")),
            book_source_type=_coerce_int(normalized.get("bookSourceType")),
            respond_time=_coerce_int(normalized.get("respondTime")),
            enabled=_as_bool(normalized.get("enabled", True)),
            icon=_as_text(normalized.get("sourceIcon") or normalized.get("bookSourceIcon")),
            sort_url=_as_text(normalized.get("sortUrl")),
            single_url=_as_text(normalized.get("singleUrl")),
            #  94.4% 的订阅源开着这个开关，所以默认 False 但几乎总会被显式打开
            load_with_base_url=_as_bool(normalized.get("loadWithBaseUrl", False)),
            _groups=groups,
        )
        return spec

    # ------------------------------------------------------------------ 取规则

    def rule(self, group: str, name: str, default: str = "") -> str:
        return self._groups.get(group, {}).get(name, "") or default

    def group_rules(self, group: str) -> Dict[str, str]:
        return dict(self._groups.get(group, {}))

    def has_rule(self, group: str, name: str) -> bool:
        return bool(self.rule(group, name).strip())

    # -------------------------------------------------------------- URL 相关

    @property
    def base_url(self) -> str:
        """已在 `#` 处截断、去掉尾部 `/` 的站点根地址。"""
        return self.url

    def absolute(self, path: str) -> str:
        """把采集阶段被剪掉 base_url 的相对地址拼回去。

        `searchUrl` 可能长成 `/search?q={{key}}`，也可能本来就是绝对地址
        （没被剪掉，或源里写的是另一个域名），两种都要支持。
        """
        text = (path or "").strip()
        if not text:
            return ""
        if text.startswith(("http://", "https://")):
            return text
        if text.startswith("//"):
            scheme = self.base_url.split("://", 1)[0] if "://" in self.base_url else "https"
            return f"{scheme}:{text}"
        if not self.base_url:
            return text
        if text.startswith(("/", ":", "?", "&", "#")):
            #  `:80/home/json`、`?key={{key}}` —— 剪 base_url 时连端口/查询串的
            #  分隔符一起留下了，直接拼回去，中间不能再补斜杠。
            return f"{self.base_url}{text}"
        return f"{self.base_url}/{text}"

    @property
    def search_urls(self) -> List[str]:
        """`searchUrl` 也可能是多行 `名称::URL`（少见但存在）。

        **不能直接按行切**：尾部的选项 JSON 经常是格式化过的多行文本，例如

            /e/search/index.php,{
              "charset": "gbk",
              "method": "POST",
              "body": "keyboard={{key}}&show=title"
            }

        按行切会得到 `/e/search/index.php,{`，请求必然 404。所以先把尾部选项摘
        出来，只对剩下的部分做多行判断，最后再把选项拼回每条 URL。
        """
        raw = self.search_url.strip()
        if not raw:
            return []

        head, options = split_url_options(raw)
        suffix = f",{json.dumps(options, ensure_ascii=False)}" if options else ""

        named = parse_named_urls(head)
        if len(named) <= 1:
            return [head.strip() + suffix]
        return [url + suffix for _, url in named if url]

    def explore_kinds(self) -> List[ExploreKind]:
        """发现页分类。`exploreUrl` 是 JSON 数组时也认。"""
        text = self.explore_url.strip()
        if not text:
            return []
        if text.startswith("["):
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, list):
                return [
                    ExploreKind(
                        name=_as_text(item.get("title") or item.get("name")),
                        url=_as_text(item.get("url")),
                        style=item.get("style") if isinstance(item, Mapping) else None,
                    )
                    for item in parsed
                    if isinstance(item, Mapping)
                ]
        return [ExploreKind(name=name, url=url) for name, url in parse_named_urls(text) if url]

    # ------------------------------------------------------------ 体检（选源用）

    @property
    def is_rss(self) -> bool:
        return self.source_type == "rss"

    @property
    def is_web_view(self) -> bool:
        """WebView 型订阅源（`singleUrl` 非空）。纯 Python 跑不了，占归档 21.9%。

        这类源不是「规则不全」—— 它的规则可能齐，但工作方式是把一个页面塞进
        WebView 里让用户直接看。引擎对它必须显式抛 `WebViewNotSupportedError`，
        不能静默返空，否则界面上和「这个源今天没更新」分不出来。
        """
        return bool(self.single_url.strip())

    @property
    def rss_categories(self) -> List[ExploreKind]:
        """`sortUrl` 的多分类入口。没有 `sortUrl` 时退化成单分类 = 源地址本身。"""
        kinds = [
            ExploreKind(name=name, url=url)
            for name, url in parse_named_urls(self.sort_url)
            if url
        ]
        if kinds:
            return kinds
        return [ExploreKind(name=self.name or "全部", url=self.url)] if self.url else []

    def missing_core_fields(self) -> List[str]:
        """缺哪些核心链路字段。空列表 = 规则完整。

        实测 19% 的书源代表源是空壳（只有 url + name），选源前必须用这个过滤掉，
        否则「JS-free 比例」会虚高约 5 个百分点。订阅源的核心集是另一套
        （`RSS_CORE_FIELDS`，只有三个）—— 拿书源那八个字段去量 RSS 源，`is_complete`
        永远是 False，一个订阅源都进不了候选池。
        """
        fields = RSS_CORE_FIELDS if self.is_rss else CORE_FIELDS
        missing: List[str] = []
        for path in fields:
            if path == ("searchUrl",):
                value = self.search_url
            elif path == ("sourceUrl",):
                value = self.url
            else:
                value = self.rule(*path)
            if not value.strip():
                missing.append(".".join(path))
        return missing

    @property
    def is_complete(self) -> bool:
        return not self.missing_core_fields()

    def needs_js(self) -> bool:
        """静态扫描全部规则串有没有 JS。零网络、零求值。"""
        if self.is_rss:
            rules = [self.url, self.sort_url, self.single_url, self.login_url]
            groups = _RSS_RULE_GROUPS
        else:
            rules = [self.search_url, self.explore_url, self.login_url]
            groups = _RULE_GROUPS
        for group in groups:
            rules.extend(self._groups.get(group, {}).values())
        return scan_features(rules)["needs_js"]

    def __repr__(self) -> str:  # pragma: no cover - 调试用
        return f"SourceSpec({self.name!r}, {self.url!r})"


def _coerce_int(value: Any) -> int:
    if value is None or isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    try:
        return int(str(value).strip() or 0)
    except ValueError:
        return 0


def _variants(data: Mapping[str, Any]) -> List[Mapping[str, Any]]:
    """列出包装对象里的全部候选源，按可信度排序。

    `apps/funread-dat/hubs/book/source/<bucket>/<url_id>.json` 的实际形状是：

        {"url_id": …, "hostname": …, "status": 2, "available": true,
         "final": false,                       ← 布尔标记，**不是**源对象
         "merged":    [{"md5_list": […], "source": {…}}, …],   ← LLM 合并产物
         "candidate": [{"md5_list": […], "source": {…}}, …]}   ← 原始去重变体

    一个 url_id 常有上百个 `candidate`（同一站点的历史版本），`merged` 是合并
    流程挑出来的结果，优先用它。注意 `final` 是 bool —— 曾经把它当源对象去取，
    于是每个源都解析成空壳、八个核心字段全缺。
    """
    out: List[Mapping[str, Any]] = []
    for key in ("merged", "candidate"):
        bucket = data.get(key)
        if isinstance(bucket, Mapping):
            bucket = [bucket]
        if not isinstance(bucket, list):
            continue
        for entry in bucket:
            if not isinstance(entry, Mapping):
                continue
            #  `{"md5_list": …, "source": {…}}` 的内层，或者直接就是源
            inner = entry.get("source")
            candidate = inner if isinstance(inner, Mapping) else entry
            if isinstance(candidate, Mapping) and candidate:
                out.append(candidate)
    return out


def _core_score(data: Mapping[str, Any]) -> int:
    """粗算一个候选源凑齐了几个核心链路字段。只用来在同一 url_id 的变体间排序。"""
    try:
        spec = SourceSpec.from_dict(data)
    except Exception:
        return -1
    return len(CORE_FIELDS) - len(spec.missing_core_fields())


def _unwrap(data: Mapping[str, Any]) -> Dict[str, Any]:
    """从包装对象里取出真正的源。已经是裸源就原样返回（只剔掉包装键）。"""
    if not isinstance(data, Mapping):
        return {}

    variants = _variants(data)
    if variants:
        #  选字段最全的那个变体。实测「挑 JS 最少的变体」没有红利（63.9%→65.7%），
        #  所以这里只按完整度选，不为 JS 做特殊处理。
        best = max(variants, key=_core_score)
        return {k: v for k, v in best.items() if k not in _WRAPPER_KEYS}

    #  裸源：`source` 单层包装也在这里剥掉
    inner = data.get("source")
    current = inner if isinstance(inner, Mapping) and inner else data
    return {k: v for k, v in current.items() if k not in _WRAPPER_KEYS}


def _normalize_top(data: Mapping[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key, value in data.items():
        target = _TOP_ALIASES.get(key, key)
        if target in out and not _is_blank(out[target]):
            continue
        out[target] = value
    return out


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (Mapping, list, tuple)):
        return len(value) == 0
    return False


def _collect_flat_rules(
    data: Mapping[str, Any],
    mapping: Mapping[str, Tuple[str, str]],
) -> Dict[str, Dict[str, str]]:
    """把扁平字段收进对应的组。

    迭代的是**映射表**而不是源数据：好几个旧字段名指向同一个规范名
    （`ruleNextPage`/`ruleNextArticles`、`ruleContentUrl`/`ruleContentUrlNext`），
    按源 JSON 的键序决定谁赢会让同一个源在不同序列化下解析出不同结果。
    按声明顺序来，优先级就是本文件里写死的那个。
    """
    groups: Dict[str, Dict[str, str]] = {}
    for key, (group, name) in mapping.items():
        text = _as_text(data.get(key))
        if not text.strip():
            continue
        bucket = groups.setdefault(group, {})
        if not bucket.get(name, "").strip():
            bucket[name] = text
    return groups


def _normalize_rss_groups(data: Mapping[str, Any]) -> Dict[str, Dict[str, str]]:
    """订阅源的规则归一化。

    RSS 源在 Legado 里全是扁平字段，但 3.x 允许写成 `ruleRss: {...}`，所以嵌套
    形态也认，并让它覆盖扁平值。
    """
    groups = _collect_flat_rules(data, _RSS_FLAT_TO_GROUP)
    raw_group = data.get("ruleRss")
    if isinstance(raw_group, Mapping):
        rules = dict(groups.get("ruleRss", {}))
        for key, value in raw_group.items():
            name = _RSS_GROUP_ALIASES.get(key, key)
            text = _as_text(value)
            if text.strip():
                rules[name] = text
        if rules:
            groups["ruleRss"] = rules
    return groups


def _normalize_groups(
    data: Mapping[str, Any],
    source_type: str = "book",
) -> Dict[str, Dict[str, str]]:
    """归一化规则组。两套表按 `source_type` 完全分派。

    不混着来：RSS 源的 `ruleContent` 是一条字符串规则，书源的 `ruleContent` 是一个
    组；共用一张表会让 RSS 源长出 `ruleToc`，也会让书源的正文规则被当成 RSS 正文。
    """
    if source_type == "rss":
        return _normalize_rss_groups(data)
    #  扁平字段先铺底，嵌套结构（3.x 的规范形态）覆盖它。
    groups: Dict[str, Dict[str, str]] = _collect_flat_rules(data, _FLAT_TO_GROUP)
    for group in _RULE_GROUPS:
        raw_group = data.get(group)
        rules: Dict[str, str] = dict(groups.get(group, {}))
        if isinstance(raw_group, Mapping):
            for key, value in raw_group.items():
                name = _GROUP_ALIASES.get(group, {}).get(key, key)
                text = _as_text(value)
                if text.strip():
                    rules[name] = text
        elif isinstance(raw_group, str) and raw_group.strip():
            # `ruleContent` 整体是一条字符串 → 当成该组的主字段
            primary = {
                "ruleContent": "content",
                "ruleToc": "chapterList",
                "ruleSearch": "bookList",
                "ruleExplore": "bookList",
                "ruleBookInfo": "intro",
            }[group]
            rules[primary] = raw_group
        if rules:
            groups[group] = rules
    return groups


def _repair_content_rule(groups: Dict[str, Dict[str, str]]) -> None:
    """纠正采集阶段 `ruleBookContent → ruleBookInfo.content` 的误映射。

    `ruleBookContent` 在 Legado 里是**章节正文**规则，不是详情页字段。采集侧
    （`manage/download/sources/book.py:71`）把它塞进了 `ruleBookInfo.content`，
    这里按 Legado 语义搬回 `ruleContent.content` —— 只在后者为空时搬，避免覆盖
    源里本来就有的正确规则。
    """
    book_info = groups.get("ruleBookInfo") or {}
    stray = book_info.get("content", "")
    if not stray.strip():
        return
    content = groups.setdefault("ruleContent", {})
    if not content.get("content", "").strip():
        content["content"] = stray
    book_info.pop("content", None)
    if not book_info:
        groups.pop("ruleBookInfo", None)


def load_source(data: Mapping[str, Any], *, source_type: str = "book") -> SourceSpec:
    return SourceSpec.from_dict(data, source_type=source_type)


__all__ = ["CORE_FIELDS", "RSS_CORE_FIELDS", "SourceSpec", "load_source"]
