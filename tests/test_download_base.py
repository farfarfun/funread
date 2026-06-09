from pathlib import Path

import funread.legado.manage.download.pipeline.task as pipeline_task_module
import funread.legado.manage.download.reporting.remote as remote_module
import funread.legado.manage.download.sources.book as book_module
import funread.legado.manage.download.sources.rss as rss_module

from funread.legado.manage.download.core import EXPORT_BATCH_SIZE, LocalSourceStore, SourceProcessor
from funread.legado.manage.download.pipeline import (
    BackupSourceDataTask,
    FetchSourceDataTask,
    PublishSourceReportTask,
    RestoreSourceDataTask,
    RunSourcePipelineTask,
    UploadSourceBatchesTask,
)
from funread.legado.manage.download.pipeline.context import SourcePipelineContext
from funread.legado.manage.download.sources.book import BookSourceProcessor
from funread.legado.manage.source import SourceMergeRunner, add_source_detail_url
import funread.legado.manage.source.merge.task as merge_module


class DummySourceProcessor(SourceProcessor):
    def loader(self) -> None:
        return None

    def source_format(self, source):
        return source


def test_loads_reads_url_map_from_source_table(tmp_path: Path) -> None:
    db_url = f"sqlite:///{tmp_path / 'download_base.db'}"
    add_source_detail_url(
        url="https://a.example", source_type="rss", source_id=10000007, database_url=db_url
    )
    add_source_detail_url(
        url="https://b.example", source_type="rss", source_id=10000008, database_url=db_url
    )
    source = DummySourceProcessor(path=str(tmp_path), cate1="rss", database_url=db_url)

    source.loads()

    assert source.url_map == {"https://a.example": 10000007, "https://b.example": 10000008}
    assert source.current_id == 10000008


def test_url_index_writes_to_source_table(tmp_path: Path) -> None:
    db_url = f"sqlite:///{tmp_path / 'download_base_write.db'}"
    source = DummySourceProcessor(path=str(tmp_path), cate1="rss", database_url=db_url)

    source.loads()
    url_id = source.url_index("https://c.example")

    assert url_id == 10000000
    assert source.url_map["https://c.example"] == 10000000


def test_book_source_download_accepts_book_source_url(tmp_path: Path) -> None:
    source = BookSourceProcessor(path=str(tmp_path), cate1="book")

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
    source = book_module.BookSourceProcessor(path=str(tmp_path), cate1="book")

    monkeypatch.setattr(
        book_module,
        "iter_source_list_data",
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
    source = rss_module.RSSSourceProcessor(path=str(tmp_path), cate1="rss")

    monkeypatch.setattr(
        rss_module,
        "iter_source_list_data",
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


class _FakeDrive:
    def __init__(self, fail_threshold=None):
        self.fail_threshold = fail_threshold
        self.calls = []

    def upload_file(self, content, fid, filepath, filename):
        payload = remote_module.json.loads(content)
        self.calls.append((filename, len(payload)))
        if self.fail_threshold is not None and len(payload) > self.fail_threshold:
            raise RuntimeError("GitHub API返回422: Sorry, the file is too large to be processed.")


def test_upload_batch_splits_large_payloads() -> None:
    context = SourcePipelineContext.__new__(SourcePipelineContext)
    context.dir_path = "funread/legado/snapshot/lasted/book"
    context.drive = _FakeDrive(fail_threshold=2)
    context._source_count_cache = {}
    manager = remote_module.SourceRemoteManager(
        context=context, initial_counter=1000, min_upload_batch_size=1
    )

    next_counter = manager.upload_batch([{"i": i} for i in range(5)], 1000)

    assert next_counter == 1003
    assert context.drive.calls == [
        ("progress-1000.json", 5),
        ("progress-1000.json", 2),
        ("progress-1001.json", 2),
        ("progress-1002.json", 1),
    ]


def test_upload_batch_increments_counter_on_success() -> None:
    context = SourcePipelineContext.__new__(SourcePipelineContext)
    context.dir_path = "funread/legado/snapshot/lasted/book"
    context.drive = _FakeDrive()
    context._source_count_cache = {}
    manager = remote_module.SourceRemoteManager(
        context=context, initial_counter=1000, min_upload_batch_size=1
    )

    next_counter = manager.upload_batch([{"i": 1}, {"i": 2}], 1000)

    assert next_counter == 1001
    assert context.drive.calls == [("progress-1000.json", 2)]


def test_source_pipeline_context_formats_size_and_counts_sources() -> None:
    context = SourcePipelineContext.__new__(SourcePipelineContext)
    context._source_count_cache = {}

    assert SourcePipelineContext.format_file_size(context, 512) == "512 B"
    assert SourcePipelineContext.format_file_size(context, 1536) == "1.5 KB"
    assert SourcePipelineContext.format_file_size(context, 1024 * 1024) == "1.0 MB"
    assert (
        SourcePipelineContext.extract_source_count(context, {"fid": "a/b.json", "name": "b.json"})
        == "-"
    )
    context._remember_source_count("a/b.json", "b.json", count=3)
    assert (
        SourcePipelineContext.extract_source_count(context, {"fid": "a/b.json", "name": "b.json"})
        == "3"
    )


def test_upload_single_batch_caches_source_count() -> None:
    context = SourcePipelineContext.__new__(SourcePipelineContext)
    context.dir_path = "funread/legado/snapshot/lasted/book"
    context._source_count_cache = {}
    context.drive = _FakeDrive()
    manager = remote_module.SourceRemoteManager(
        context=context, initial_counter=1000, min_upload_batch_size=1
    )

    manager.upload_single_batch([{"i": 1}, {"i": 2}], 1000)

    assert (
        SourcePipelineContext.extract_source_count(
            context,
            {
                "fid": "funread/legado/snapshot/lasted/book/progress-1000.json",
                "name": "progress-1000.json",
            },
        )
        == "2"
    )


def test_source_step_tasks_delegate_to_generator() -> None:
    calls = []

    class _Store:
        def __enter__(self):
            calls.append(("enter",))
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            calls.append(("exit",))

        def loader(self):
            calls.append(("download",))

        def dumps_zip(self):
            calls.append(("compress",))
            return "backup.tar.xz"

        def loads_zip(self, zip_file=None):
            calls.append(("extract", zip_file))

        def export_sources(self, size):
            calls.append(("export", size))
            yield [{"i": 1}, {"i": 2}]

    class _RemoteManager:
        def upload_exported_sources(self, runner, export_batch_size):
            calls.append(("upload", export_batch_size, runner.__class__.__name__))

        def publish_html_report(self, html_content):
            calls.append(("rss", html_content))

    class _ReportBuilder:
        def generate_html_report(self):
            calls.append(("render_report",))
            return "<html></html>"

    store = _Store()
    remote_manager = _RemoteManager()
    report_builder = _ReportBuilder()

    FetchSourceDataTask(store=store).run()
    assert BackupSourceDataTask(store=store).run() == "backup.tar.xz"
    RestoreSourceDataTask(store=store, zip_file="a.tar.xz").run()
    UploadSourceBatchesTask(store=store, remote_manager=remote_manager).run()
    PublishSourceReportTask(report_builder=report_builder, remote_manager=remote_manager).run()

    assert calls == [
        ("enter",),
        ("download",),
        ("exit",),
        ("enter",),
        ("compress",),
        ("exit",),
        ("enter",),
        ("extract", "a.tar.xz"),
        ("exit",),
        ("enter",),
        ("upload", EXPORT_BATCH_SIZE, "_Store"),
        ("exit",),
        ("render_report",),
        ("rss", "<html></html>"),
    ]


def test_run_source_pipeline_task_runs_pipeline_in_order(monkeypatch) -> None:
    calls = []

    class _Store:
        pass

    class _RemoteManager:
        pass

    class _ReportBuilder:
        pass

    class _Context:
        def __init__(self):
            self.remote_manager = _RemoteManager()
            self.report_builder = _ReportBuilder()

        def create_store(self, path):
            calls.append(("create_store", path))
            return _Store()

    monkeypatch.setattr(
        RunSourcePipelineTask,
        "get_cache_root",
        staticmethod(lambda: "/tmp/cache"),
    )
    monkeypatch.setattr(
        RunSourcePipelineTask,
        "build_context",
        lambda self, source_type: _Context(),
    )

    class _BaseStep:
        step_name = ""

        def __init__(self, *args, **kwargs):
            self.store = kwargs.get("store")
            self.remote_manager = kwargs.get("remote_manager")
            self.report_builder = kwargs.get("report_builder")

        def run(self):
            payload = self.store if self.store is not None else self.report_builder
            calls.append((self.step_name, type(payload).__name__ if payload is not None else None))

    class _Download(_BaseStep):
        step_name = "download"

    class _Compress(_BaseStep):
        step_name = "compress"

    class _Extract(_BaseStep):
        step_name = "extract"

    class _Upload(_BaseStep):
        step_name = "upload"

    class _Rss(_BaseStep):
        step_name = "rss"

    monkeypatch.setattr(pipeline_task_module, "FetchSourceDataTask", _Download)
    monkeypatch.setattr(pipeline_task_module, "BackupSourceDataTask", _Compress)
    monkeypatch.setattr(pipeline_task_module, "RestoreSourceDataTask", _Extract)
    monkeypatch.setattr(pipeline_task_module, "UploadSourceBatchesTask", _Upload)
    monkeypatch.setattr(pipeline_task_module, "PublishSourceReportTask", _Rss)

    RunSourcePipelineTask().run_book()

    assert calls == [
        ("create_store", "/tmp/cache"),
        ("download", "_Store"),
        ("compress", "_Store"),
        ("extract", "_Store"),
        ("upload", "_Store"),
        ("rss", "_ReportBuilder"),
    ]


def test_cleanup_stale_remote_batches_deletes_higher_counters() -> None:
    context = SourcePipelineContext.__new__(SourcePipelineContext)
    context.dir_path = "funread/legado/snapshot/lasted/book"

    class _File:
        def __init__(self, name, fid):
            self.name = name
            self.fid = fid

    class _Drive:
        def __init__(self):
            self.deleted = []

        def get_file_list(self, fid):
            return [
                _File("progress-1000.json", "a/1000"),
                _File("progress-1002.json", "a/1002"),
                _File("progress-1003.json", "a/1003"),
                _File("index.html", "a/index"),
            ]

        def delete(self, fid):
            self.deleted.append(fid)
            return True

    context.drive = _Drive()
    manager = remote_module.SourceRemoteManager(
        context=context, initial_counter=1000, min_upload_batch_size=1
    )

    manager.cleanup_stale_remote_batches(1002)

    assert context.drive.deleted == ["a/1002", "a/1003"]


def test_source_merge_runner_merges_candidates_back_to_source_file(tmp_path: Path) -> None:
    store = BookSourceProcessor(path=str(tmp_path), cate1="book")
    source_dir = Path(store.path_bok) / "10000000-10000100"
    source_dir.mkdir(parents=True, exist_ok=True)
    source_path = source_dir / "10000001.json"
    source_path.write_text(
        """
        {
          "available": true,
          "merged": [],
          "candidate": [
            {
              "md5_list": ["old-1"],
              "source": {
                "bookSourceName": "书源A",
                "bookSourceUrl": "https://books.example.com/api/",
                "ruleSearchUrl": "https://books.example.com/api/search"
              }
            },
            {
              "md5_list": ["old-2"],
              "source": {
                "bookSourceName": "书源A增强",
                "bookSourceUrl": "https://books.example.com/api/",
                "ruleExploreUrl": "https://books.example.com/api/explore"
              }
            }
          ],
          "final": false,
          "url_id": 10000001,
          "hostname": "books.example.com"
        }
        """,
        encoding="utf-8",
    )

    class FakeMerger:
        def merge_sources(self, source_type, hostname, versions):
            assert source_type == "book"
            assert hostname == "books.example.com"
            assert len(versions) == 2
            return {
                "bookSourceName": "书源A最终",
                "bookSourceUrl": "https://books.example.com/api/",
                "ruleSearchUrl": "https://books.example.com/api/search",
                "ruleFindUrl": "https://books.example.com/api/explore",
            }

    stats = SourceMergeRunner(store=store, merger=FakeMerger()).run()

    data = LocalSourceStore._load_json_safely(str(source_path))
    assert stats == {"processed": 1, "merged": 1, "skipped": 0, "failed": 0}
    assert data["candidate"] == []
    assert len(data["merged"]) == 1
    assert data["merged"][0]["source"]["bookSourceUrl"] == "https://books.example.com/api"
    assert data["merged"][0]["source"]["searchUrl"] == "/search"
    assert data["merged"][0]["source"]["ruleExplore"]["url"] == "/explore"
    assert data["merged"][0]["md5_list"][1:] == ["old-1", "old-2"]


def test_source_merge_runner_rejects_hostname_drift(tmp_path: Path) -> None:
    store = rss_module.RSSSourceProcessor(path=str(tmp_path), cate1="rss")
    source_dir = Path(store.path_bok) / "10000000-10000100"
    source_dir.mkdir(parents=True, exist_ok=True)
    source_path = source_dir / "10000002.json"
    original = {
        "available": True,
        "merged": [],
        "candidate": [
            {
                "md5_list": ["rss-1"],
                "source": {
                    "sourceName": "RSS A",
                    "sourceUrl": "https://rss.example.com/feed/",
                },
            },
            {
                "md5_list": ["rss-2"],
                "source": {
                    "sourceName": "RSS B",
                    "sourceUrl": "https://rss.example.com/feed/",
                },
            },
        ],
        "final": False,
        "url_id": 10000002,
        "hostname": "rss.example.com",
    }
    source_path.write_text(remote_module.json.dumps(original, ensure_ascii=False), encoding="utf-8")

    class FakeMerger:
        def merge_sources(self, source_type, hostname, versions):
            return {
                "sourceName": "Bad RSS",
                "sourceUrl": "https://other.example.com/feed/",
            }

    stats = merge_module.SourceMergeRunner(store=store, merger=FakeMerger()).run()

    data = LocalSourceStore._load_json_safely(str(source_path))
    assert stats == {"processed": 1, "merged": 0, "skipped": 0, "failed": 1}
    assert data == original
