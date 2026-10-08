"""引擎的出入参数据结构。

全部是朴素 dataclass，不带任何 ORM / pydantic 依赖 —— `engine/` 不许碰
sqlalchemy（见 `engine/__init__.py` 的分层禁令）。持久化是 `reader/` 的事。
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class SearchBook:
    """搜索/发现结果里的一本书。字段大多可空 —— 真实源很少给全。"""

    name: str = ""
    author: str = ""
    book_url: str = ""
    kind: str = ""
    word_count: str = ""
    last_chapter: str = ""
    intro: str = ""
    cover_url: str = ""
    source_url: str = ""
    source_name: str = ""
    variables: Dict[str, str] = field(default_factory=dict)

    @property
    def is_usable(self) -> bool:
        """没有 name 或没有 book_url 的条目没法往下走，直接丢掉。"""
        return bool(self.name.strip() and self.book_url.strip())


@dataclass
class BookInfo:
    """书籍详情页。"""

    name: str = ""
    author: str = ""
    book_url: str = ""
    toc_url: str = ""
    kind: str = ""
    word_count: str = ""
    last_chapter: str = ""
    intro: str = ""
    cover_url: str = ""
    source_url: str = ""
    source_name: str = ""
    can_rename: bool = False
    #  `@put` 捕获的变量要随详情一起持久化：不然「详情里 put、目录里 get」
    #  在换源或重启后全读到空值。
    variables: Dict[str, str] = field(default_factory=dict)


@dataclass
class Chapter:
    """目录里的一章。"""

    index: int = 0
    name: str = ""
    url: str = ""
    update_time: str = ""
    is_vip: bool = False
    is_pay: bool = False
    variables: Dict[str, str] = field(default_factory=dict)


@dataclass
class ChapterContent:
    """章节正文。`pages` 保留分页原文，便于排查翻页规则。"""

    text: str = ""
    title: str = ""
    url: str = ""
    pages: List[str] = field(default_factory=list)
    next_url: str = ""


@dataclass
class RssArticle:
    """订阅源里的一篇文章。"""

    title: str = ""
    link: str = ""
    pub_date: str = ""
    description: str = ""
    image: str = ""
    content: str = ""
    source_url: str = ""
    source_name: str = ""
    variables: Dict[str, str] = field(default_factory=dict)


@dataclass
class ExploreKind:
    """发现页的一个分类入口（`名称::URL` 里的一行）。"""

    name: str = ""
    url: str = ""
    style: Optional[Dict[str, Any]] = None


__all__ = [
    "BookInfo",
    "Chapter",
    "ChapterContent",
    "ExploreKind",
    "RssArticle",
    "SearchBook",
]
