class BaseParse:
    def __init__(self, source=None, *args, **kwargs):
        self.source = source

    def parse_list(self, page_no, page_size=10, *args, **kwargs):
        pass

    def parse_detail(self, path):
        pass
