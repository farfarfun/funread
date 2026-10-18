"""SourceReportBuilder 的真实渲染。

tests/test_download_base.py 里那个 `_ReportBuilder` 是假类，真正的
SourceReportBuilder（context.py:40 的生产路径）在 2026-10-18 之前一行都没被跑过。
这里不碰 GithubDrive —— 只给一个假的 context，验证 HTML 真能生成、字段真进了表。
"""

from funread.legado.manage.download.reporting import SourceReportBuilder


class _Drive:
    def __init__(self, files, fail=False):
        self.files = files
        self.fail = fail

    def get_file_list(self, _path):
        if self.fail:
            raise RuntimeError("网络断了")
        return self.files


class _Context:
    source_type = "bookSource"
    repo_str = "farfarfun/funread-cache"
    dir_path = "snapshot/x"

    def __init__(self, files, fail=False):
        self.drive = _Drive(files, fail)
        self._source_count_cache = {}

    def _remember_source_count(self, *keys, count):
        for key in keys:
            if key:
                self._source_count_cache[str(key)] = str(count)


FILES = [
    {"name": "book-0001.json", "fid": "f1", "size": 2048, "source_count": 12},
    {"name": "readme.txt", "fid": "f2", "size": 10},
]


def test_report_contains_a_row_per_json_file_with_import_links():
    html = SourceReportBuilder(_Context(FILES)).generate_html_report()
    assert "<html>" in html and "gridtable" in html
    assert "book-0001.json" in html and "readme.txt" not in html
    assert (
        "yuedu://bookSource/importonline?src=https://github.com/farfarfun/funread-cache/raw/master/f1"
        in html
    )
    assert "https://gitee.com/farfarfun/funread-cache/raw/master/f1" in html
    assert "2.0 KB" in html and ">12<" in html
    assert "Build Information" in html


def test_a_drive_failure_still_yields_a_report():
    """表格拿不到就空着，报告其余部分照样出 —— 这是发邮件用的，不能整封丢。"""
    html = SourceReportBuilder(_Context(FILES, fail=True)).generate_html_report()
    assert "<html>" in html and "book-0001.json" not in html and "Build Information" in html


def test_format_file_size_units():
    fmt = SourceReportBuilder.format_file_size
    assert fmt(0) == "0 B"
    assert fmt(1023) == "1023 B"
    assert fmt(1024) == "1.0 KB"
    assert fmt(5 * 1024 * 1024) == "5.0 MB"
    assert fmt("abc") == "abc"


def test_extract_source_count_caches_by_fid_and_name():
    ctx = _Context(FILES)
    builder = SourceReportBuilder(ctx)
    assert builder.extract_source_count(FILES[0]) == "12"
    assert ctx._source_count_cache == {"f1": "12", "book-0001.json": "12"}
    #  第二次走缓存，哪怕文件对象里的数字变了
    assert builder.extract_source_count({**FILES[0], "source_count": 99}) == "12"
    assert builder.extract_source_count(FILES[1]) == "-"
    assert builder.extract_source_count({"name": "x.json", "fid": "f9"}) == "-"
