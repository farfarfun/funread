"""源存储工厂：新增一种源类型只需在此注册一个处理器类。"""

from ..core.store import LocalSourceStore
from .book import BookSourceProcessor
from .rss import RSSSourceProcessor

_REGISTRY: dict[str, tuple[type[LocalSourceStore], str]] = {}


def register_source_type(
    source_type: str, processor_cls: type[LocalSourceStore], cate1: str
) -> None:
    """注册一个源类型对应的处理器类及其本地存储子目录。

    Args:
        source_type: 源类型标识（如 ``"booksource"``、``"rsssource"``）。
        processor_cls: 对应的 :class:`LocalSourceStore` 子类。
        cate1: 本地存储使用的一级子目录名（如 ``"book"``、``"rss"``）。
    """
    _REGISTRY[source_type] = (processor_cls, cate1)


def supported_source_types() -> list[str]:
    """返回当前已注册的所有源类型标识，按字典序排列。

    Returns:
        已注册源类型标识组成的列表。
    """
    return sorted(_REGISTRY)


class SourceStoreFactory:
    """根据源类型构建对应的本地源存储实例。"""

    @staticmethod
    def create(path: str, source_type: str) -> LocalSourceStore:
        """创建指定源类型的本地存储实例。

        Args:
            path: 本地存储根目录。
            source_type: 源类型标识，必须已通过 :func:`register_source_type` 注册。

        Returns:
            对应的 :class:`LocalSourceStore` 子类实例。

        Raises:
            ValueError: 当 ``source_type`` 未注册时抛出。
        """
        entry = _REGISTRY.get(source_type)
        if entry is None:
            raise ValueError(f"Unsupported source_type: {source_type}")
        processor_cls, cate1 = entry
        return processor_cls(path=path, cate1=cate1)


register_source_type("booksource", BookSourceProcessor, cate1="book")
register_source_type("rsssource", RSSSourceProcessor, cate1="rss")
