from pathlib import Path

from funread.legado.manage.download.base import DownloadSource
from funread.legado.manage.download.book import BookSourceDownload


class DummyDownloadSource(DownloadSource):
    def loader(self) -> None:
        return None

    def source_format(self, source):
        return source


def test_loads_normalizes_parallel_url_lists(tmp_path: Path) -> None:
    source = DummyDownloadSource(path=str(tmp_path), cate1="rss")
    Path(source.pkl_url).write_text(
        '{"data":{"url":["https://a.example","https://b.example"],"url_id":[["7"],8]}}',
        encoding="utf-8",
    )

    source.loads()

    assert source.url_map == {"https://a.example": 7, "https://b.example": 8}
    assert source.current_id == 8


def test_loads_skips_invalid_url_map_items(tmp_path: Path) -> None:
    source = DummyDownloadSource(path=str(tmp_path), cate1="rss")
    Path(source.pkl_url).write_text(
        '{"data":[{"url":"https://a.example","url_id":[9]},{"url":"https://b.example","url_id":[]},{"url":"","url_id":11}]}',
        encoding="utf-8",
    )

    source.loads()

    assert source.url_map["https://a.example"] == 9
    assert "https://b.example" not in source.url_map
    assert source.current_id == max(source.url_map.values())


def test_book_source_download_accepts_book_source_url(tmp_path: Path) -> None:
    source = BookSourceDownload(path=str(tmp_path), cate1="book")

    added = source.add_source(
        {
            "bookSourceUrl": "https://books.example.com/api/",
            "bookSourceName": "Example Book",
        }
    )

    assert added is True
    exported = next(source.export_sources(size=10))
    assert exported[0]["bookSourceUrl"].startswith("https://books.example.com/api#")
