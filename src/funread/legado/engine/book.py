"""书源四段流程：搜索 → 详情 → 目录 → 正文。

抓取通过注入的 `Fetcher` 完成，引擎自己不碰网络，所以这整条流程能用
`StaticFetcher` 在离线单测里跑通。

三处容易写错、这里刻意写死的语义：

- **相对 URL 的 base**：页内链接按 `page.url`（重定向后的最终地址）解析；
  `searchUrl`/`exploreUrl` 按 `source.base_url` 拼 —— 采集阶段把 base_url
  从这两个字段里剪掉了，不拼回去就发不出请求。
- **翻页终止**：URL 重复、空结果、页数上限，三条全要。真实源里 `nextTocUrl`
  指回自己是常态，只靠「空结果」会死循环。
- **变量传递**：详情页 `@put` 的变量要随 `BookInfo` 带到目录和正文阶段，否则
  「详情里 put、目录里 get」在换源或重启后全读到空。
"""

from typing import Dict, List, Optional, Sequence

from .errors import RuleEmptyError, WebViewNotSupportedError
from .evaluator import RuleEvaluator, join_url
from .fetch import Fetcher, Page, parse_url_options
from .js import JsRuntime, NullJsRuntime
from .lexer import parse_replace_regex
from .models import BookInfo, Chapter, ChapterContent, SearchBook
from .source import SourceSpec
from .variables import VariableScope

#  翻页上限。目录 50 页足够覆盖长篇连载；正文单章极少超过 10 页。
MAX_TOC_PAGES = 50
MAX_CONTENT_PAGES = 10


class BookSourceEngine:
    """一个书源的四段流程。一个实例对应一个源，持有源级变量作用域。"""

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

    def _fetch(
        self,
        raw_url: str,
        *,
        context: Optional[Dict] = None,
        base: str = "",
        variables: Optional[Dict[str, str]] = None,
    ) -> Page:
        """解析 URL 选项 → 展开模板 → 发请求。

        `variables` 必须传进来：目录/正文的 URL 里可以有 `@get:{}`，拿的是详情页
        `@put` 下的值。少了这一路，带变量的翻页 URL 会展开成空串。
        """
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

    # ------------------------------------------------------------------ 搜索

    def search(self, keyword: str, page: int = 1) -> List[SearchBook]:
        """关键词搜索。返回已过滤掉「没名字/没链接」的条目。"""
        if not self.source.search_url.strip():
            raise RuleEmptyError("该源没有 searchUrl", source_url=self.source.url)
        context = {"key": keyword, "page": page}
        results: List[SearchBook] = []
        for raw_url in self.source.search_urls:
            page_obj = self._fetch(raw_url, context=context)
            results.extend(self._parse_book_list(page_obj, "ruleSearch", context))
            if results:
                break
        return results

    def explore(self, raw_url: str, page: int = 1) -> List[SearchBook]:
        """发现页。规则组是 `ruleExplore`，缺省回落到 `ruleSearch`。"""
        context = {"key": "", "page": page}
        page_obj = self._fetch(raw_url, context=context)
        group = "ruleExplore" if self.source.has_rule("ruleExplore", "bookList") else "ruleSearch"
        return self._parse_book_list(page_obj, group, context)

    def _parse_book_list(self, page: Page, group: str, context: Dict) -> List[SearchBook]:
        scope = self._evaluator(page.text, base_url=page.url, context=context)
        list_rule = self.source.rule(group, "bookList")
        rows = scope.rows(list_rule) if list_rule.strip() else [scope]

        books: List[SearchBook] = []
        for row in rows:
            book = SearchBook(
                name=row.string(self.source.rule(group, "name")),
                author=row.string(self.source.rule(group, "author")),
                book_url=row.url(self.source.rule(group, "bookUrl")),
                kind=row.string(self.source.rule(group, "kind")),
                word_count=row.string(self.source.rule(group, "wordCount")),
                last_chapter=row.string(self.source.rule(group, "lastChapter")),
                intro=row.string(self.source.rule(group, "intro")),
                cover_url=row.url(self.source.rule(group, "coverUrl")),
                source_url=self.source.url,
                source_name=self.source.name,
                variables=row.variables.local(),
            )
            if book.is_usable:
                books.append(book)
        return books

    # ------------------------------------------------------------------ 详情

    def book_info(self, book: SearchBook) -> BookInfo:
        """书籍详情。

        Legado 的行为：搜索结果已经给全字段时可以跳过详情页请求。这里保守一些 ——
        只有在**没有任何 ruleBookInfo 规则**时才跳过，因为 tocUrl 往往只有详情页才给。
        """
        info = BookInfo(
            name=book.name,
            author=book.author,
            book_url=book.book_url,
            toc_url=book.book_url,
            kind=book.kind,
            word_count=book.word_count,
            last_chapter=book.last_chapter,
            intro=book.intro,
            cover_url=book.cover_url,
            source_url=self.source.url,
            source_name=self.source.name,
            variables=dict(book.variables),
        )
        rules = self.source.group_rules("ruleBookInfo")
        if not rules:
            return info

        page = self._fetch(book.book_url, base=self.source.base_url, variables=book.variables)
        scope = self._evaluator(
            page.text,
            base_url=page.url,
            variables=self.variables.child(book.variables),
        )

        #  `init` 是预处理规则：把作用域收缩到详情区块，没命中就留在整页上。
        narrowed = scope.scope(rules.get("init", ""))
        if narrowed is not None:
            scope = narrowed

        info.name = scope.string(rules.get("name"), default=info.name)
        info.author = scope.string(rules.get("author"), default=info.author)
        info.kind = scope.string(rules.get("kind"), default=info.kind)
        info.word_count = scope.string(rules.get("wordCount"), default=info.word_count)
        info.last_chapter = scope.string(rules.get("lastChapter"), default=info.last_chapter)
        info.intro = scope.string(rules.get("intro"), default=info.intro)
        info.cover_url = scope.url(rules.get("coverUrl")) or info.cover_url
        info.can_rename = scope.flag(rules.get("canReName"))
        info.toc_url = scope.url(rules.get("tocUrl")) or page.url
        info.variables = scope.variables.snapshot()
        return info

    # ------------------------------------------------------------------ 目录

    def toc(self, info: BookInfo) -> List[Chapter]:
        """章节目录，含 `nextTocUrl` 翻页。"""
        rules = self.source.group_rules("ruleToc")
        list_rule = rules.get("chapterList", "")
        if not list_rule.strip():
            raise RuleEmptyError("该源没有 ruleToc.chapterList", source_url=self.source.url)

        chapters: List[Chapter] = []
        next_url = info.toc_url or info.book_url
        seen: set = set()

        for _ in range(MAX_TOC_PAGES):
            if not next_url or next_url in seen:
                break
            seen.add(next_url)
            page = self._fetch(next_url, base=self.source.base_url, variables=info.variables)
            scope = self._evaluator(
                page.text,
                base_url=page.url,
                variables=self.variables.child(info.variables),
            )
            rows = scope.rows(list_rule)
            if not rows:
                break
            for row in rows:
                name = row.string(rules.get("chapterName"))
                url = row.url(rules.get("chapterUrl")) or page.url
                if not name.strip():
                    continue
                chapters.append(
                    Chapter(
                        index=len(chapters),
                        name=name,
                        url=url,
                        update_time=row.string(rules.get("updateTime")),
                        is_vip=row.flag(rules.get("isVip")),
                        is_pay=row.flag(rules.get("isPay")),
                        variables=row.variables.local(),
                    )
                )
            next_rule = rules.get("nextTocUrl", "")
            next_url = scope.url(next_rule) if next_rule.strip() else ""

        return chapters

    # ------------------------------------------------------------------ 正文

    def content(self, chapter: Chapter, *, info: Optional[BookInfo] = None) -> ChapterContent:
        """章节正文，含 `nextContentUrl` 翻页与 `replaceRegex` 净化。"""
        rules = self.source.group_rules("ruleContent")
        content_rule = rules.get("content", "")
        if not content_rule.strip():
            raise RuleEmptyError("该源没有 ruleContent.content", source_url=self.source.url)

        inherited = dict(info.variables) if info else {}
        inherited.update(chapter.variables)

        pages: List[str] = []
        next_url = chapter.url
        seen: set = set()
        last_next = ""

        for _ in range(MAX_CONTENT_PAGES):
            if not next_url or next_url in seen:
                break
            seen.add(next_url)
            page = self._fetch(next_url, base=self.source.base_url, variables=inherited)
            scope = self._evaluator(
                page.text,
                base_url=page.url,
                context={"title": chapter.name},
                variables=self.variables.child(inherited),
            )
            text = scope.string(content_rule)
            if text.strip():
                pages.append(text)
            next_rule = rules.get("nextContentUrl", "")
            next_url = scope.url(next_rule) if next_rule.strip() else ""
            last_next = next_url

        body = "\n".join(pages)
        for spec in parse_replace_regex(rules.get("replaceRegex", "")):
            body = spec.apply(body)

        return ChapterContent(
            text=_tidy_content(body),
            title=chapter.name,
            url=chapter.url,
            pages=pages,
            next_url=last_next,
        )


def _tidy_content(text: str) -> str:
    """正文排版：去掉行首尾空白、压掉连续空行，保留段落分隔。"""
    lines = [line.strip() for line in (text or "").splitlines()]
    out: List[str] = []
    for line in lines:
        if not line and (not out or not out[-1]):
            continue
        out.append(line)
    while out and not out[-1]:
        out.pop()
    return "\n".join(out)


def pick_best(books: Sequence[SearchBook], name: str, author: str = "") -> Optional[SearchBook]:
    """从搜索结果里挑最匹配的一本（书名完全相同优先，再比作者）。"""
    target_name = (name or "").strip()
    target_author = (author or "").strip()
    best: Optional[SearchBook] = None
    best_score = -1
    for book in books:
        score = 0
        if book.name.strip() == target_name:
            score += 2
        elif target_name and target_name in book.name:
            score += 1
        if target_author and book.author.strip() == target_author:
            score += 2
        if score > best_score:
            best, best_score = book, score
    return best


__all__ = [
    "MAX_CONTENT_PAGES",
    "MAX_TOC_PAGES",
    "BookSourceEngine",
    "pick_best",
]
