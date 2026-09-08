"""Source store factory. Adding a new source type = register one class here."""

from typing import Dict, Tuple, Type

from .book import BookSourceProcessor
from .rss import RSSSourceProcessor
from ..core.store import LocalSourceStore

_REGISTRY: Dict[str, Tuple[Type[LocalSourceStore], str]] = {}


def register_source_type(
    source_type: str, processor_cls: Type[LocalSourceStore], cate1: str
) -> None:
    """Register a processor class + its local storage sub-directory (``cate1``)."""
    _REGISTRY[source_type] = (processor_cls, cate1)


def supported_source_types() -> list:
    return sorted(_REGISTRY)


class SourceStoreFactory:
    """Build concrete local source stores from source type."""

    @staticmethod
    def create(path: str, source_type: str):
        entry = _REGISTRY.get(source_type)
        if entry is None:
            raise ValueError(f"Unsupported source_type: {source_type}")
        processor_cls, cate1 = entry
        return processor_cls(path=path, cate1=cate1)


register_source_type("booksource", BookSourceProcessor, cate1="book")
register_source_type("rsssource", RSSSourceProcessor, cate1="rss")
