"""订阅源的服务层。和 `service.py`（书源）平行的一层。

两条腿，对外是同一组方法，由订阅的 `kind` 分派：

- `kind="legado"` → 归档里的 Legado RSS 源，走 `engine/rss.py` 的规则求值。
  纯 Python 可跑的只有 142 个（归档 1,344 个的 10.6%）。
- `kind="feed"`   → 用户自己贴的标准 feed 地址，走 `engine/feed.py` 的 XML 解析。
  **100% 可用** —— 标准 feed 没有规则也没有 JS。

分派只影响「怎么拿到一页 `RssArticle`」。订阅表、已读状态、错误记账全是共用的，
因为对用户来说两者就是「我的订阅」里的两行，没有交互差别。
"""

import threading
from typing import Any, Dict, List, Optional

from farlog import getLogger

from ..engine import (
    JsNotSupportedError,
    RssArticle,
    RssPage,
    RssSourceEngine,
    RuleSyntaxError,
    SourceSpec,
    UnsupportedFeatureError,
    WebViewNotSupportedError,
    feed_title,
    parse_feed,
)
from ..net import RequestsFetcher
from . import storage
from .registry import SourceRegistry

logger = getLogger("funread")

#: 抓取超时 `(连接, 读取)`。订阅列表比书源搜索宽松一点 —— 它是单源请求，
#: 没有「等最慢的那个拖住整批」的问题。
FETCH_TIMEOUT = (6, 20)

#: feed 响应大小上限。用户给的是任意 URL，没有上限就等于把内存交给对方。
MAX_FEED_BYTES = 8 * 1024 * 1024

KIND_LEGADO = "legado"
KIND_FEED = "feed"


def _default_fetcher(timeout=FETCH_TIMEOUT) -> RequestsFetcher:
    return RequestsFetcher(timeout=timeout)


class RssService:
    """订阅源的门面。一个进程一个实例就够。"""

    def __init__(
        self,
        cache_root: Optional[str] = None,
        database_url: Optional[str] = None,
        registry: Optional[SourceRegistry] = None,
        allow_web_view: bool = False,
        fetcher_factory: Optional[Any] = None,
    ):
        self.database_url = database_url
        self.registry = registry or SourceRegistry(
            cache_root=cache_root, source_type="rss", database_url=database_url
        )
        self.allow_web_view = allow_web_view
        #  可替换，好让单测注入 `StaticFetcher` 而不打网
        self.fetcher_factory = fetcher_factory or _default_fetcher
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ 源目录

    def available_sources(self, limit: int = 50, offset: int = 0, q: str = "") -> Dict[str, Any]:
        """候选池里的 Legado 订阅源，供「订阅源目录」页浏览。

        只列 `enabled` 的 —— 那已经排除了规则不全、需要 JS 和 `singleUrl` 型。
        这是本地查表加本地文件读取，不打网。
        """
        keyword = (q or "").strip()
        rows = self.registry.prefs(limit=None)
        items: List[Dict[str, Any]] = []
        for pref in rows:
            if keyword and keyword not in (pref.name or ""):
                continue
            items.append(
                {
                    "url_id": int(pref.url_id),
                    "name": pref.name or "",
                    "needs_js": bool(pref.needs_js),
                }
            )
        total = len(items)
        window = items[offset : offset + limit]
        #  分类和图标要读源文件才有，所以只给当前这一页补 —— 全量补等于把
        #  上千个 JSON 读一遍。
        for item in window:
            spec = self.registry.load_spec(item["url_id"])
            if spec is None:
                continue
            item["group"] = spec.group
            item["icon"] = spec.icon
            item["categories"] = [kind.name for kind in spec.rss_categories]
        return {"items": window, "total": total, "limit": limit, "offset": offset}

    # ------------------------------------------------------------------ 订阅

    def _spec(self, url_id: int) -> SourceSpec:
        spec = self.registry.load_spec(url_id)
        if spec is None:
            raise LookupError(f"订阅源不存在或无法解析：url_id={url_id}")
        return spec

    def subscribe_legado(self, url_id: int, user_id: int, title: str = "") -> Dict[str, Any]:
        """订阅归档里的一个 Legado 源。

        订阅前就把源解析一遍并拒掉 `singleUrl` 型：让用户订上一个点开永远是
        错误提示的源，比当场说「这个源需要浏览器环境」要糟得多。
        """
        spec = self._spec(url_id)
        if spec.is_web_view and not self.allow_web_view:
            raise WebViewNotSupportedError(
                "该订阅源需要浏览器环境，暂不支持", source_url=spec.url
            )
        if not spec.is_complete:
            raise RuleSyntaxError(
                f"该订阅源规则不完整，缺少：{'、'.join(spec.missing_core_fields())}"
            )
        sub_id = storage.upsert_subscription(
            {
                "kind": KIND_LEGADO,
                "url_id": int(url_id),
                "title": title or spec.name or spec.url,
                "icon": spec.icon,
                "group": spec.group,
            },
            user_id=user_id,
            database_url=self.database_url,
        )
        return self.subscription(sub_id, user_id)

    def subscribe_feed(self, feed_url: str, user_id: int, title: str = "") -> Dict[str, Any]:
        """订阅一个用户给的标准 feed 地址。

        **先抓一次并解析**，失败就不入库：这个地址是用户手打的，九成的失败是
        贴错了（贴了网页首页而不是 feed）。当场给出「这是一个 HTML 页面而不是
        feed 地址」，比订上之后每次刷新都空着要有用得多。

        注意这个方法会让服务端去拉**任意** URL（SSRF），所以调用方必须把它放在
        登录态之后，且不随「公开阅读端」开关放开。
        """
        url = (feed_url or "").strip()
        if not url.lower().startswith(("http://", "https://")):
            raise RuleSyntaxError("feed 地址必须以 http:// 或 https:// 开头")

        payload = self._download(url)
        page = parse_feed(payload, source_url=url)
        resolved = title or feed_title(payload, default="") or url

        sub_id = storage.upsert_subscription(
            {"kind": KIND_FEED, "feed_url": url, "title": resolved[:255]},
            user_id=user_id,
            database_url=self.database_url,
        )
        storage.record_subscription_fetch(
            sub_id, user_id=user_id, database_url=self.database_url
        )
        result = self.subscription(sub_id, user_id)
        result["preview"] = len(page.items)
        return result

    def _download(self, url: str) -> bytes:
        """抓一个 feed 地址。大小设上限 —— 用户给的是任意 URL。"""
        from ..engine import Request

        with self.fetcher_factory(timeout=FETCH_TIMEOUT) as fetcher:
            page = fetcher.fetch(Request(url=url))
        body = page.text or ""
        data = body.encode("utf-8", "ignore") if isinstance(body, str) else body
        if len(data) > MAX_FEED_BYTES:
            raise RuleSyntaxError(
                f"feed 内容超过 {MAX_FEED_BYTES // 1024 // 1024}MB，拒绝解析"
            )
        return data

    def unsubscribe(self, sub_id: str, user_id: int) -> bool:
        return storage.remove_subscription(
            sub_id, user_id=user_id, database_url=self.database_url
        )

    def subscription(self, sub_id: str, user_id: int) -> Dict[str, Any]:
        row = storage.get_subscription(sub_id, user_id=user_id, database_url=self.database_url)
        if row is None:
            raise LookupError("没有这个订阅")
        return self._to_dict(row)

    def subscriptions(self, user_id: int) -> List[Dict[str, Any]]:
        return [
            self._to_dict(row)
            for row in storage.list_subscriptions(
                user_id=user_id, database_url=self.database_url
            )
        ]

    def _to_dict(self, row) -> Dict[str, Any]:
        return {
            "sub_id": row.sub_id,
            "kind": row.kind,
            "url_id": int(row.url_id or 0),
            "feed_url": row.feed_url or "",
            "title": row.title or "",
            "icon": row.icon or "",
            "group": row.group or "",
            "last_fetched_at": (
                row.last_fetched_at.isoformat() if row.last_fetched_at else ""
            ),
            "last_error": row.last_error or "",
            "read_count": storage.count_read(
                row.sub_id, user_id=row.user_id, database_url=self.database_url
            ),
        }

    # ------------------------------------------------------------------ 分类

    def categories(self, sub_id: str, user_id: int) -> List[Dict[str, str]]:
        """订阅的分类入口。标准 feed 没有分类概念，返回单项。"""
        row = storage.get_subscription(sub_id, user_id=user_id, database_url=self.database_url)
        if row is None:
            raise LookupError("没有这个订阅")
        if row.kind != KIND_LEGADO:
            return [{"name": row.title or "全部", "url": row.feed_url or ""}]
        spec = self._spec(int(row.url_id))
        return [{"name": kind.name, "url": kind.url} for kind in spec.rss_categories]

    # ------------------------------------------------------------------ 列表

    def articles(
        self,
        sub_id: str,
        user_id: int,
        category: str = "",
        next_url: str = "",
        page: int = 1,
        limit: int = 30,
    ) -> Dict[str, Any]:
        """一页文章，带上每篇的已读 / 收藏状态。

        抓取成败都记进订阅行（`last_error`），这样界面能解释「为什么这个订阅是
        空的」而不是干摆一个空列表。
        """
        row = storage.get_subscription(sub_id, user_id=user_id, database_url=self.database_url)
        if row is None:
            raise LookupError("没有这个订阅")

        try:
            if row.kind == KIND_LEGADO:
                result = self._articles_legado(row, category, next_url, page)
            else:
                result = self._articles_feed(row)
        except Exception as exc:
            storage.record_subscription_fetch(
                sub_id, user_id=user_id, error=str(exc), database_url=self.database_url
            )
            raise
        storage.record_subscription_fetch(
            sub_id, user_id=user_id, database_url=self.database_url
        )

        items = result.items[:limit]
        keys = [storage.compute_article_key(a.link) for a in items if a.link]
        states = storage.get_article_states(
            sub_id, keys, user_id=user_id, database_url=self.database_url
        )
        payload = []
        for article in items:
            key = storage.compute_article_key(article.link) if article.link else ""
            state = states.get(key)
            payload.append(
                {
                    "article_key": key,
                    "title": article.title,
                    "link": article.link,
                    "pub_date": article.pub_date,
                    "description": article.description,
                    "image": article.image,
                    "read": bool(state.read) if state else False,
                    "favorited": bool(state.favorited) if state else False,
                    "variables": dict(article.variables),
                }
            )
        return {
            "items": payload,
            "next_url": result.next_url,
            "category": result.category,
            #  有没有正文规则决定界面是「点进去读」还是「点进去跳浏览器」
            "has_content": self._has_content_rule(row),
        }

    def _articles_legado(self, row, category: str, next_url: str, page: int) -> RssPage:
        spec = self._spec(int(row.url_id))
        with self.fetcher_factory(timeout=FETCH_TIMEOUT) as fetcher:
            engine = RssSourceEngine(spec, fetcher, allow_web_view=self.allow_web_view)
            return engine.articles(category or None, page=page, next_url=next_url)

    def _articles_feed(self, row) -> RssPage:
        #  标准 feed 一次给全部条目，没有翻页概念 —— next_url 恒为空。
        return parse_feed(self._download(row.feed_url), source_url=row.feed_url)

    def _has_content_rule(self, row) -> bool:
        if row.kind != KIND_LEGADO:
            #  feed 的正文（content:encoded / summary）在列表里就带着了
            return True
        spec = self.registry.load_spec(int(row.url_id))
        if spec is None:
            return False
        return bool(
            spec.rule("ruleRss", "content").strip() or spec.rule("ruleRss", "description").strip()
        )

    # ------------------------------------------------------------------ 正文

    def article(
        self,
        sub_id: str,
        user_id: int,
        link: str,
        variables: Optional[Dict[str, str]] = None,
        title: str = "",
    ) -> Dict[str, Any]:
        """一篇文章的正文。

        标准 feed 的正文在列表响应里就有，所以这里要重抓一次 feed 再按链接找 ——
        看着浪费，但订阅文章不缓存正文（时效性强、量大、收益低），而重抓一次
        feed 比维护一份会过期的正文缓存简单得多。
        """
        row = storage.get_subscription(sub_id, user_id=user_id, database_url=self.database_url)
        if row is None:
            raise LookupError("没有这个订阅")

        if row.kind == KIND_LEGADO:
            spec = self._spec(int(row.url_id))
            try:
                with self.fetcher_factory(timeout=FETCH_TIMEOUT) as fetcher:
                    engine = RssSourceEngine(
                        spec, fetcher, allow_web_view=self.allow_web_view
                    )
                    article = engine.article(link, variables=variables, title=title)
            except (JsNotSupportedError, UnsupportedFeatureError):
                #  结构性不支持：把源停用，下次选它结果一样
                storage.record_source_result(
                    "rss",
                    int(row.url_id),
                    ok=False,
                    error="正文规则需要 JS 或浏览器环境",
                    disable_after=1,
                    database_url=self.database_url,
                )
                raise
            storage.record_source_result(
                "rss", int(row.url_id), ok=True, database_url=self.database_url
            )
        else:
            article = self._feed_article(row, link)

        return {
            "article_key": storage.compute_article_key(article.link or link),
            "title": article.title or title,
            "link": article.link or link,
            "pub_date": article.pub_date,
            "description": article.description,
            "image": article.image,
            "content_html": article.content,
        }

    def _feed_article(self, row, link: str) -> RssArticle:
        page = parse_feed(self._download(row.feed_url), source_url=row.feed_url)
        for item in page.items:
            if item.link == link:
                return item
        raise LookupError("这篇文章已不在 feed 里")

    # ------------------------------------------------------------------ 状态

    def mark_read(
        self,
        sub_id: str,
        user_id: int,
        article_key: str,
        read: bool = True,
        meta: Optional[Dict[str, str]] = None,
    ) -> None:
        storage.set_article_state(
            sub_id,
            article_key,
            user_id=user_id,
            read=read,
            meta=meta,
            database_url=self.database_url,
        )

    def mark_favorite(
        self,
        sub_id: str,
        user_id: int,
        article_key: str,
        favorited: bool = True,
        meta: Optional[Dict[str, str]] = None,
    ) -> None:
        storage.set_article_state(
            sub_id,
            article_key,
            user_id=user_id,
            favorited=favorited,
            meta=meta,
            database_url=self.database_url,
        )

    def mark_all_read(
        self,
        sub_id: str,
        user_id: int,
        article_keys: List[str],
        metas: Optional[Dict[str, Dict[str, str]]] = None,
    ) -> int:
        return storage.mark_all_read(
            sub_id,
            article_keys,
            user_id=user_id,
            metas=metas,
            database_url=self.database_url,
        )

    def favorites(self, user_id: int, sub_id: str = "") -> List[Dict[str, Any]]:
        return [
            {
                "sub_id": row.sub_id,
                "article_key": row.article_key,
                "title": row.title,
                "link": row.link,
                "pub_date": row.pub_date,
                "image": row.image,
                "read": bool(row.read),
            }
            for row in storage.list_favorites(
                user_id=user_id, sub_id=sub_id or None, database_url=self.database_url
            )
        ]


__all__ = ["FETCH_TIMEOUT", "KIND_FEED", "KIND_LEGADO", "MAX_FEED_BYTES", "RssService"]
