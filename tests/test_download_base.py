from pathlib import Path

import funread.legado.manage.download.book as book_module
import funread.legado.manage.download.rss as rss_module

from funread.legado.manage.download.base import DownloadSource
from funread.legado.manage.download.book import BookSourceDownload
from funread.legado.manage.source_download import add_source_url


class DummyDownloadSource(DownloadSource):
    def loader(self) -> None:
        return None

    def source_format(self, source):
        return source


def test_loads_reads_url_map_from_source_table(tmp_path: Path) -> None:
    db_url = f"sqlite:///{tmp_path / 'download_base.db'}"
    add_source_url(url="https://a.example", source_id=10000007, database_url=db_url)
    add_source_url(url="https://b.example", source_id=10000008, database_url=db_url)
    source = DummyDownloadSource(path=str(tmp_path), cate1="rss", database_url=db_url)

    source.loads()

    assert source.url_map == {"https://a.example": 10000007, "https://b.example": 10000008}
    assert source.current_id == 10000008


def test_url_index_writes_to_source_table(tmp_path: Path) -> None:
    db_url = f"sqlite:///{tmp_path / 'download_base_write.db'}"
    source = DummyDownloadSource(path=str(tmp_path), cate1="rss", database_url=db_url)

    source.loads()
    url_id = source.url_index("https://c.example")

    assert url_id == 10000000
    assert source.url_map["https://c.example"] == 10000000


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


def test_book_loader_reads_source_download_iterator(monkeypatch, tmp_path: Path) -> None:
    source = book_module.BookSourceDownload(path=str(tmp_path), cate1="book")

    monkeypatch.setattr(
        book_module,
        "iter_source_download_data",
        lambda source_type: iter(
            [
                (
                    object(),
                    {
                        "bookSourceUrl": "https://books.example.com/api/",
                        "bookSourceName": "Example Book",
                    },
                )
            ]
        ),
    )

    source.loader()

    exported = next(source.export_sources(size=10))
    assert exported[0]["bookSourceUrl"].startswith("https://books.example.com/api#")


def test_rss_loader_reads_source_download_iterator(monkeypatch, tmp_path: Path) -> None:
    source = rss_module.RSSSourceDownload(path=str(tmp_path), cate1="rss")

    monkeypatch.setattr(
        rss_module,
        "iter_source_download_data",
        lambda source_type: iter(
            [
                (
                    object(),
                    {"sourceUrl": "https://rss.example.com/feed/", "sourceName": "Example RSS"},
                )
            ]
        ),
    )

    source.loader()

    exported = next(source.export_sources(size=10))
    assert exported[0]["sourceUrl"].startswith("https://rss.example.com/feed#")
