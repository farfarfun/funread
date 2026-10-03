from typing import Any


class BaseParse:
    """网页解析器抽象基类。"""

    def __init__(self, source: Any = None, *args: Any, **kwargs: Any) -> None:
        """初始化解析器。

        Args:
            source: 解析器使用的数据源。
            *args: 子类可选的位置参数。
            **kwargs: 子类可选的关键字参数。
        """
        self.source = source

    def parse_list(self, page_no: int, page_size: int = 10, *args: Any, **kwargs: Any) -> Any:
        """解析分页列表。

        Args:
            page_no: 页码。
            page_size: 每页记录数。
            *args: 子类可选的位置参数。
            **kwargs: 子类可选的关键字参数。

        Returns:
            子类定义的分页解析结果。
        """
        raise NotImplementedError("子类必须实现 parse_list")

    def parse_detail(self, path: str) -> Any:
        """解析详情页。

        Args:
            path: 详情页路径或 URL。

        Returns:
            子类定义的详情解析结果。
        """
        raise NotImplementedError("子类必须实现 parse_detail")
