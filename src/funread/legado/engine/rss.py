"""订阅源两段流程：文章列表 → 文章正文。

和 `book.py` 同一套写法（注入 `Fetcher`，引擎不碰网络，所以能用 `StaticFetcher`
离线跑通），但订阅源有三处和书源不一样，都是实测归档 1,344 个源之后定下来的：

- **一次只返回一页。** 书源目录要把所有页走完才能给出完整章节表；订阅列表是
  「加载更多」的交互，而且源动辄几十页，一次全抓既慢又没人看。所以
  `articles()` 返回 `RssPage(items, next_url)`，由调用方决定要不要接着拿。
- **`singleUrl` 型源必须显式报错。** 占归档 21.9%，工作方式是把一个页面塞进
  WebView 让用户直接看，没有可解析的列表结构。静默返空会让界面上「这个源不
  支持」和「这个源今天没更新」长得一模一样。
- **`ruleLink` 只有 40.3% 的源有。** 缺了就拿列表项自身的 `href` 兜底 —— 那是
  RSS 条目的常态结构，兜得住，所以 `ruleLink` 不进 `RSS_CORE_FIELDS`。

`loadWithBaseUrl`（94.4% 的源开着）在正文阶段把相对链接按页面地址绝对化，
否则正文里的图片和站内链接在我们这边全是死的。
"""

from typing import Dict, List, Optional

from .errors import RuleEmptyError, WebViewNotSupportedError
from .evaluator import RuleEvaluator, join_url
from .fetch import Fetcher, Page, parse_url_options
from .js import JsRuntime, NullJsRuntime
from .models import ExploreKind, RssArticle, RssPage
from .source import SourceSpec
from .variables import VariableScope

#: 一次 `articles(...)` 调用内部最多跟几跳。正常是 1 —— 调用方拿 `next_url`
#: 自己翻。只有 `follow=True` 时才会连翻，给「一次抓完」的后台任务用。
MAX_ARTICLE_PAGES = 20

#: `ruleLink` 缺失时的兜底取值规则。RSS 列表项基本都是 `<a href=...>` 结构。
_LINK_FALLBACK = "href"


def absolutize_html(html: str, base_url: str) -> str:
    """把 HTML 片段里的相对链接按 `base_url` 绝对化。

    给 `loadWithBaseUrl` 用。纯 lxml，没有新依赖。解析不动就原样返回 —— 正文
    拿不到图片比正文整段丢掉要好得多。
    """
    text = (html or "").strip()
    if not text or not base_url:
        return html or ""
    try:
        from lxml import html as lxml_html

        fragment = lxml_html.fragment_fromstring(text, create_parent="div")
        fragment.make_links_absolute(base_url, resolve_base_href=True)
        #  只要内部 HTML：create_parent 包的那层 div 不是源站内容
        inner = (fragment.text or "") + "".join(
            lxml_html.tostring(child, encoding="unicode") for child in fragment
        )
        return inner
    except Exception:
        return html or ""


class RssSourceEngine:
    """一个订阅源的两段流程。一个实例对应一个源，持有源级变量作用域。"""

    def __init__(
        self,
        source: SourceSpec,
        fetcher: Fetcher,
        *,
        js: Optional[JsRuntime] = None,
        allow_web_view: bool = False,
    ):
        self.source = source
        self.fetcher = fetcher
        self.js = js or NullJsRuntime()
        self.allow_web_view = allow_web_view
        self.variables = VariableScope()

    # ------------------------------------------------------------------ 抓取

    def _reject_web_view(self) -> None:
        if self.source.is_web_view and not self.allow_web_view:
            raise WebViewNotSupportedError(
                "该订阅源是 singleUrl（WebView）型，需要浏览器环境，当前阶段不支持",
                rule=self.source.single_url,
                source_url=self.source.url,
            )

    def _fetch(
        self,
        raw_url: str,
        *,
        context: Optional[Dict] = None,
        base: str = "",
        variables: Optional[Dict[str, str]] = None,
    ) -> Page:
        """解析 URL 选项 → 展开模板 → 发请求。和 `book.py._fetch` 同义。"""
        scope = self._evaluator(
            "",
            base_url=base or self.source.base_url,
            context=context,
            variables=self.variables.child(variables or {}),
        )
        request = parse_url_options(raw_url, default_headers=self.source.header)
        request.url = scope.template(request.url)
        if request.body:
            request.body = scope.template(request.body)

        if request.web_view and not self.allow_web_view:
            raise WebViewNotSupportedError(
                "该规则要求 WebView 渲染，当前阶段不支持",
                rule=raw_url,
                source_url=self.source.url,
            )

        request.url = self.source.absolute(request.url) if not base else join_url(base, request.url)
        return self.fetcher.fetch(request)

    def _evaluator(
        self,
        content,
        *,
        base_url: str,
        context: Optional[Dict] = None,
        variables: Optional[VariableScope] = None,
    ) -> RuleEvaluator:
        return RuleEvaluator(
            content,
            base_url=base_url,
            js=self.js,
            context=context or {},
            variables=variables if variables is not None else self.variables.child(),
        )

    # ------------------------------------------------------------------ 分类

    def categories(self) -> List[ExploreKind]:
        """`sortUrl` 的多分类入口。没有 `sortUrl` 时是单分类 = 源地址本身。

        零网络：`sortUrl` 是源 JSON 里的静态字段。
        """
        return self.source.rss_categories

    def _resolve_start_url(self, category: Optional[str]) -> str:
        """分类名或分类 URL → 要抓的地址。

        既认分类**名称**（前端从 `categories()` 里取到的那个）也认 URL 本身，
        因为「加载更多」回传的是 URL。都不匹配时退回第一个分类。
        """
        kinds = self.categories()
        if not kinds:
            raise RuleEmptyError("该订阅源没有 sourceUrl", source_url=self.source.url)
        if not category:
            return kinds[0].url
        for kind in kinds:
            if category in (kind.name, kind.url):
                return kind.url
        #  给的是个没登记过的 URL：按原样抓。订阅源的 sortUrl 不一定穷举了所有
        #  入口，拒绝反而把能用的路径堵死。
        return category

    # ------------------------------------------------------------------ 列表

    def articles(
        self,
        category: Optional[str] = None,
        *,
        page: int = 1,
        next_url: str = "",
        follow: bool = False,
    ) -> RssPage:
        """一页文章。

        `next_url` 优先于 `category`：它就是上一页返回的「下一页」地址。
        `follow=True` 时在一次调用内把后续页都跟完（给后台任务用），仍受
        `MAX_ARTICLE_PAGES` 与「URL 重复」「空列表」三重终止保护。
        """
        self._reject_web_view()
        rules = self.source.group_rules("ruleRss")
        list_rule = rules.get("articles", "")
        if not list_rule.strip():
            raise RuleEmptyError("该订阅源没有 ruleArticles", source_url=self.source.url)

        start = next_url.strip() or self._resolve_start_url(category)
        context = {"key": "", "page": page}

        items: List[RssArticle] = []
        seen: set = set()
        current = start
        following = ""

        for _ in range(MAX_ARTICLE_PAGES if follow else 1):
            if not current or current in seen:
                break
            seen.add(current)
            page_obj = self._fetch(current, context=context, base=self.source.base_url)
            scope = self._evaluator(page_obj.text, base_url=page_obj.url, context=context)
            rows = scope.rows(list_rule)
            if not rows:
                break
            items.extend(self._parse_rows(rows, rules, page_obj))

            next_rule = rules.get("nextPage", "")
            following = scope.url(next_rule) if next_rule.strip() else ""
            #  真实源里 nextPage 指回当前页是常态；放过去会让调用方死循环
            if following in seen:
                following = ""
            if not follow:
                break
            current = following

        return RssPage(items=items, next_url=following, category=category or "")

    def _parse_rows(self, rows, rules: Dict[str, str], page: Page) -> List[RssArticle]:
        articles: List[RssArticle] = []
        for row in rows:
            title = row.string(rules.get("title"))
            if not title.strip():
                #  没标题的条目在界面上是一行空白，不如丢掉
                continue
            articles.append(
                RssArticle(
                    title=title,
                    link=self._link_of(row, rules, page),
                    pub_date=row.string(rules.get("pubDate")),
                    description=row.string(rules.get("description")),
                    image=row.url(rules.get("image")),
                    source_url=self.source.url,
                    source_name=self.source.name,
                    variables=row.variables.local(),
                )
            )
        return articles

    def _link_of(self, row, rules: Dict[str, str], page: Page) -> str:
        """文章链接。`ruleLink` 缺失（59.7% 的源）时拿列表项自身的 href 兜底。"""
        link_rule = rules.get("link", "")
        if link_rule.strip():
            link = row.url(link_rule)
            if link:
                return link
        fallback = row.url(_LINK_FALLBACK)
        #  兜底也拿不到就退回页面地址：至少点进去能看到列表页本身，
        #  比一个空链接（点了没反应）要好。
        return fallback or page.url

    # ------------------------------------------------------------------ 正文

    def article(
        self,
        link: str,
        *,
        variables: Optional[Dict[str, str]] = None,
        title: str = "",
    ) -> RssArticle:
        """抓一篇文章的正文。

        `ruleContent` 只有 67.6% 的源有；没有时退回 `ruleDescription`（2.0%），
        两者都没有就抛 `RuleEmptyError` —— 这类源只能看列表，界面该说清楚，
        不该给一篇空白文章。
        """
        self._reject_web_view()
        if not (link or "").strip():
            raise RuleEmptyError("没有文章链接", source_url=self.source.url)

        rules = self.source.group_rules("ruleRss")
        content_rule = rules.get("content", "") or rules.get("description", "")
        if not content_rule.strip():
            raise RuleEmptyError(
                "该订阅源没有 ruleContent，只能看列表", source_url=self.source.url
            )

        page = self._fetch(link, base=self.source.base_url, variables=variables or {})
        scope = self._evaluator(
            page.text,
            base_url=page.url,
            variables=self.variables.child(variables or {}),
        )
        html = scope.string(content_rule)
        if self.source.load_with_base_url:
            html = absolutize_html(html, page.url)

        return RssArticle(
            title=title or scope.string(rules.get("title")) or "",
            link=page.url or link,
            pub_date=scope.string(rules.get("pubDate")),
            description=scope.string(rules.get("description")),
            image=scope.url(rules.get("image")),
            content=html,
            source_url=self.source.url,
            source_name=self.source.name,
            variables=scope.variables.snapshot(),
        )


__all__ = ["MAX_ARTICLE_PAGES", "RssSourceEngine", "absolutize_html"]
