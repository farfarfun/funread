class BaseParse:
    """网页解析器抽象基类。"""
    def __init__(self, source=None, *args, **kwargs):
        self.source = source

    def parse_list(self, page_no: int, page_size: int = 10, *args, **kwargs):
        """解析分页列表，具体解析器必须实现。"""
        raise NotImplementedError("子类必须实现 parse_list")

    def parse_detail(self, path: str):
        """解析详情页，具体解析器必须实现。"""
        raise NotImplementedError("子类必须实现 parse_detail")
