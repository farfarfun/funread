"""Source store factory."""

from .book import BookSourceProcessor
from .rss import RSSSourceProcessor


class SourceStoreFactory:
    """Build concrete local source stores from source type."""

    @staticmethod
    def create(path: str, source_type: str):
        if source_type == "booksource":
            return BookSourceProcessor(path=path, cate1="book")
        if source_type == "rsssource":
            return RSSSourceProcessor(path=path, cate1="rss")
        raise ValueError(f"Unsupported source_type: {source_type}")
