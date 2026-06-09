from pathlib import Path

import funread.legado.manage.download.reporting.remote as remote_module
import funread.legado.manage.download.sources.book as book_module
import funread.legado.manage.download.sources.rss as rss_module
import funread.legado.manage.download.task as generate_task_module
import funread.legado.manage.source.merge.task as merge_module

from funread.legado.manage.download.core import EXPORT_BATCH_SIZE, LocalSourceStore, SourceProcessor
from funread.legado.manage.download import (
    DownloadSourceDataTask,
    DumpSourceBackupTask,
    GenerateSourceTask,
    LoadSourceBackupTask,
    PublishSourceReportTask,
    UploadSourceBatchesTask,
)
from funread.legado.manage.download.context import SourceBuildContext
from funread.legado.manage.download.sources.book import BookSourceProcessor
from funread.legado.manage.source import (
    SourceMergeRunner,
    SyncLocalSourceRecordsTask,
    add_source_detail_url,
    list_source_detail_records,
    load_source_index_map,
    upsert_source_index_records,
)


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


def test_add_source_skips_existing_md5_from_database(tmp_path: Path) -> None:
    db_url = f"sqlite:///{tmp_path / 'source_index.db'}"
    source = BookSourceProcessor(path=str(tmp_path), cate1="book", database_url=db_url)
    normalized_source = {
        "bookSourceUrl": "https://books.example.com/api",
        "bookSourceName": "ExampleBook",
        "bookSourceComment": "",
    }
    md5 = source.compute_source_md5(normalized_source)
    upsert_source_index_records(
        records=[
            {
                "md5": md5,
                "source_type": "book",
                "url_id": 10000001,
                "hostname": "books.example.com",
                "cate1": 10000000,
            }
        ],
        source_type="book",
        database_url=db_url,
    )

    source.loads()
    added = source.add_source(
        {
            "bookSourceUrl": "https://books.example.com/api/",
            "bookSourceName": "Example Book",
        }
    )

    assert added is False


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
    context = SourceBuildContext.__new__(SourceBuildContext)
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
    context = SourceBuildContext.__new__(SourceBuildContext)
    context.dir_path = "funread/legado/snapshot/lasted/book"
    context.drive = _FakeDrive()
    context._source_count_cache = {}
    manager = remote_module.SourceRemoteManager(
        context=context, initial_counter=1000, min_upload_batch_size=1
    )

    next_counter = manager.upload_batch([{"i": 1}, {"i": 2}], 1000)

    assert next_counter == 1001
    assert context.drive.calls == [("progress-1000.json", 2)]


def test_source_build_context_formats_size_and_counts_sources() -> None:
    context = SourceBuildContext.__new__(SourceBuildContext)
    context._source_count_cache = {}

    assert SourceBuildContext.format_file_size(context, 512) == "512 B"
    assert SourceBuildContext.format_file_size(context, 1536) == "1.5 KB"
    assert SourceBuildContext.format_file_size(context, 1024 * 1024) == "1.0 MB"
    assert (
        SourceBuildContext.extract_source_count(context, {"fid": "a/b.json", "name": "b.json"})
        == "-"
    )
    context._remember_source_count("a/b.json", "b.json", count=3)
    assert (
        SourceBuildContext.extract_source_count(context, {"fid": "a/b.json", "name": "b.json"})
        == "3"
    )


def test_upload_single_batch_caches_source_count() -> None:
    context = SourceBuildContext.__new__(SourceBuildContext)
    context.dir_path = "funread/legado/snapshot/lasted/book"
    context._source_count_cache = {}
    context.drive = _FakeDrive()
    manager = remote_module.SourceRemoteManager(
        context=context, initial_counter=1000, min_upload_batch_size=1
    )

    manager.upload_single_batch([{"i": 1}, {"i": 2}], 1000)

    assert (
        SourceBuildContext.extract_source_count(
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

    DownloadSourceDataTask(store=store).run()
    assert DumpSourceBackupTask(store=store).run() == "backup.tar.xz"
    LoadSourceBackupTask(store=store, zip_file="a.tar.xz").run()
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


def test_generate_source_task_runs_in_order(monkeypatch) -> None:
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
        GenerateSourceTask,
        "get_cache_root",
        staticmethod(lambda: "/tmp/cache"),
    )
    monkeypatch.setattr(
        GenerateSourceTask,
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

    class _Merge:
        def __init__(self, *args, **kwargs):
            self.store = kwargs.get("store")

        def run(self, limit=None):
            calls.append(("merge", type(self.store).__name__ if self.store is not None else None))
            return {"processed": 0, "merged": 0, "skipped": 0, "failed": 0}

    class _Upload(_BaseStep):
        step_name = "upload"

    class _Rss(_BaseStep):
        step_name = "rss"

    class _Sync:
        def __init__(self, *args, **kwargs):
            self.path = kwargs.get("path")

        def run_source(self, source_type, database_url=None):
            calls.append(("sync", source_type, self.path, database_url))

    monkeypatch.setattr(generate_task_module, "LoadSourceBackupTask", _Extract)
    monkeypatch.setattr(generate_task_module, "DownloadSourceDataTask", _Download)
    monkeypatch.setattr(generate_task_module, "SourceMergeRunner", _Merge)
    monkeypatch.setattr(generate_task_module, "DumpSourceBackupTask", _Compress)
    monkeypatch.setattr(generate_task_module, "SyncLocalSourceRecordsTask", _Sync)
    monkeypatch.setattr(generate_task_module, "UploadSourceBatchesTask", _Upload)
    monkeypatch.setattr(generate_task_module, "PublishSourceReportTask", _Rss)

    GenerateSourceTask().run_book(
        load=True, download=True, merge=True, dump=True, sync=True, upload=True, publish=True
    )

    assert calls == [
        ("create_store", "/tmp/cache"),
        ("extract", "_Store"),
        ("download", "_Store"),
        ("merge", "_Store"),
        ("compress", "_Store"),
        ("sync", "book", "/tmp/cache", None),
        ("upload", "_Store"),
        ("rss", "_ReportBuilder"),
    ]


def test_generate_source_task_can_skip_steps(monkeypatch) -> None:
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
        GenerateSourceTask,
        "get_cache_root",
        staticmethod(lambda: "/tmp/cache"),
    )
    monkeypatch.setattr(
        GenerateSourceTask,
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

    class _Merge:
        def __init__(self, *args, **kwargs):
            self.store = kwargs.get("store")

        def run(self, limit=None):
            calls.append(("merge", type(self.store).__name__ if self.store is not None else None))
            return {"processed": 0, "merged": 0, "skipped": 0, "failed": 0}

    class _Upload(_BaseStep):
        step_name = "upload"

    class _Sync:
        def __init__(self, *args, **kwargs):
            self.path = kwargs.get("path")

        def run_source(self, source_type, database_url=None):
            calls.append(("sync", source_type, self.path, database_url))

    monkeypatch.setattr(generate_task_module, "LoadSourceBackupTask", _BaseStep)
    monkeypatch.setattr(generate_task_module, "DownloadSourceDataTask", _Download)
    monkeypatch.setattr(generate_task_module, "SourceMergeRunner", _Merge)
    monkeypatch.setattr(generate_task_module, "DumpSourceBackupTask", _BaseStep)
    monkeypatch.setattr(generate_task_module, "SyncLocalSourceRecordsTask", _Sync)
    monkeypatch.setattr(generate_task_module, "UploadSourceBatchesTask", _Upload)
    monkeypatch.setattr(generate_task_module, "PublishSourceReportTask", _BaseStep)

    GenerateSourceTask().run_book(
        load=False, download=True, merge=False, dump=False, sync=False, upload=True, publish=False
    )

    assert calls == [
        ("create_store", "/tmp/cache"),
        ("download", "_Store"),
        ("upload", "_Store"),
    ]


def test_cleanup_stale_remote_batches_deletes_higher_counters() -> None:
    context = SourceBuildContext.__new__(SourceBuildContext)
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


def test_openai_compatible_merger_reads_json_response(monkeypatch) -> None:
    class _Response:
        status_code = 200
        text = '{"choices":[{"message":{"content":"{\\"ok\\": true}"}}]}'

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return False

        def raise_for_status(self):
            return None

        def json(self):
            return {"choices": [{"message": {"content": '{"ok": true}'}}]}

    def _fake_post(*args, **kwargs):
        return _Response()

    monkeypatch.setattr(merge_module.requests, "post", _fake_post)
    merger = merge_module.OpenAICompatibleSourceMerger(
        api_key="test-key",
        base_url="https://example.com/v1",
        model="deepseek/deepseek-reasoner",
        timeout=30,
    )

    content = merger._post_and_collect_content(
        {
            "model": "deepseek/deepseek-reasoner",
            "messages": [{"role": "user", "content": "x"}],
        }
    )

    assert content == '{"ok": true}'


def test_openai_compatible_merger_retries_non_json_response(monkeypatch) -> None:
    responses = iter(["not-json", '{"bookSourceUrl":"https://books.example.com/api/"}'])

    def _fake_post_and_collect_content(self, payload):
        return next(responses)

    monkeypatch.setattr(
        merge_module.OpenAICompatibleSourceMerger,
        "_post_and_collect_content",
        _fake_post_and_collect_content,
    )
    merger = merge_module.OpenAICompatibleSourceMerger(
        api_key="test-key",
        base_url="https://example.com/v1",
        model="deepseek/deepseek-reasoner",
        timeout=30,
        max_retries=2,
        retry_sleep_seconds=0,
    )

    result = merger.merge_sources(
        source_type="book",
        hostname="books.example.com",
        versions=[{"bookSourceUrl": "https://books.example.com/api/"}],
    )

    assert result == {"bookSourceUrl": "https://books.example.com/api/"}


def test_openai_compatible_merger_raises_after_retry_exhausted(monkeypatch) -> None:
    def _fake_post_and_collect_content(self, payload):
        return "still-not-json"

    monkeypatch.setattr(
        merge_module.OpenAICompatibleSourceMerger,
        "_post_and_collect_content",
        _fake_post_and_collect_content,
    )
    merger = merge_module.OpenAICompatibleSourceMerger(
        api_key="test-key",
        base_url="https://example.com/v1",
        model="deepseek/deepseek-reasoner",
        timeout=30,
        max_retries=2,
        retry_sleep_seconds=0,
    )

    try:
        merger.merge_sources(
            source_type="book",
            hostname="books.example.com",
            versions=[{"bookSourceUrl": "https://books.example.com/api/"}],
        )
    except ValueError as error:
        assert "after 2 attempts" in str(error)
    else:
        raise AssertionError("Expected ValueError after retry exhaustion")


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


def test_source_merge_runner_prioritizes_fewer_versions_first(tmp_path: Path) -> None:
    store = BookSourceProcessor(path=str(tmp_path), cate1="book")
    source_dir = Path(store.path_bok) / "10000000-10000100"
    source_dir.mkdir(parents=True, exist_ok=True)
    small_path = source_dir / "10000001.json"
    large_path = source_dir / "10000002.json"

    small_path.write_text(
        """
        {
          "available": true,
          "merged": [],
          "candidate": [
            {"md5_list": ["a"], "source": {"bookSourceName": "A", "bookSourceUrl": "https://a.example.com/api/"}},
            {"md5_list": ["b"], "source": {"bookSourceName": "B", "bookSourceUrl": "https://a.example.com/api/"}}
          ],
          "final": false,
          "url_id": 10000001,
          "hostname": "a.example.com"
        }
        """,
        encoding="utf-8",
    )
    large_path.write_text(
        """
        {
          "available": true,
          "merged": [],
          "candidate": [
            {"md5_list": ["a"], "source": {"bookSourceName": "A", "bookSourceUrl": "https://b.example.com/api/"}},
            {"md5_list": ["b"], "source": {"bookSourceName": "B", "bookSourceUrl": "https://b.example.com/api/"}},
            {"md5_list": ["c"], "source": {"bookSourceName": "C", "bookSourceUrl": "https://b.example.com/api/"}}
          ],
          "final": false,
          "url_id": 10000002,
          "hostname": "b.example.com"
        }
        """,
        encoding="utf-8",
    )

    class FakeMerger:
        def merge_sources(self, source_type, hostname, versions):
            return versions[0]

    runner = SourceMergeRunner(store=store, merger=FakeMerger())

    ordered = runner.iter_source_files()

    assert ordered == [str(small_path), str(large_path)]


def test_source_merge_runner_splits_large_merge_requests(tmp_path: Path) -> None:
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
            {"md5_list": ["m1"], "source": {"bookSourceName": "A", "bookSourceUrl": "https://books.example.com/api/", "ruleSearchUrl": "https://books.example.com/api/search1"}},
            {"md5_list": ["m2"], "source": {"bookSourceName": "B", "bookSourceUrl": "https://books.example.com/api/", "ruleSearchUrl": "https://books.example.com/api/search2"}},
            {"md5_list": ["m3"], "source": {"bookSourceName": "C", "bookSourceUrl": "https://books.example.com/api/", "ruleSearchUrl": "https://books.example.com/api/search3"}},
            {"md5_list": ["m4"], "source": {"bookSourceName": "D", "bookSourceUrl": "https://books.example.com/api/", "ruleSearchUrl": "https://books.example.com/api/search4"}}
          ],
          "final": false,
          "url_id": 10000001,
          "hostname": "books.example.com"
        }
        """,
        encoding="utf-8",
    )

    calls = []

    class FakeMerger:
        def merge_sources(self, source_type, hostname, versions):
            calls.append(len(versions))
            return versions[0]

    stats = SourceMergeRunner(
        store=store,
        merger=FakeMerger(),
        max_versions_per_merge=2,
        max_prompt_chars=1,
    ).run()

    assert stats == {"processed": 1, "merged": 1, "skipped": 0, "failed": 0}
    assert calls == [2, 2, 2]


def test_source_merge_runner_persists_chunk_progress_on_failure(tmp_path: Path) -> None:
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
            {"md5_list": ["m1"], "source": {"bookSourceName": "A", "bookSourceUrl": "https://books.example.com/api/", "ruleSearchUrl": "https://books.example.com/api/search1"}},
            {"md5_list": ["m2"], "source": {"bookSourceName": "B", "bookSourceUrl": "https://books.example.com/api/", "ruleSearchUrl": "https://books.example.com/api/search2"}},
            {"md5_list": ["m3"], "source": {"bookSourceName": "C", "bookSourceUrl": "https://books.example.com/api/", "ruleSearchUrl": "https://books.example.com/api/search3"}},
            {"md5_list": ["m4"], "source": {"bookSourceName": "D", "bookSourceUrl": "https://books.example.com/api/", "ruleSearchUrl": "https://books.example.com/api/search4"}}
          ],
          "final": false,
          "url_id": 10000001,
          "hostname": "books.example.com"
        }
        """,
        encoding="utf-8",
    )

    calls = []

    class FakeMerger:
        def merge_sources(self, source_type, hostname, versions):
            calls.append(len(versions))
            if len(calls) == 2:
                raise ValueError("merge chunk failed")
            return versions[0]

    stats = SourceMergeRunner(
        store=store,
        merger=FakeMerger(),
        max_versions_per_merge=2,
        max_prompt_chars=1,
    ).run()

    data = LocalSourceStore._load_json_safely(str(source_path))

    assert stats == {"processed": 1, "merged": 0, "skipped": 0, "failed": 1}
    assert calls == [2, 2]
    assert data["merged"] == []
    assert len(data["candidate"]) == 3
    assert data["candidate"][0]["source"]["bookSourceUrl"] == "https://books.example.com/api"
    assert data["candidate"][0]["md5_list"][1:] == ["m1", "m2"]
    assert data["candidate"][1]["md5_list"] == ["m3"]
    assert data["candidate"][2]["md5_list"] == ["m4"]


def test_sync_local_source_records_task_updates_mysql_tables(tmp_path: Path) -> None:
    db_url = f"sqlite:///{tmp_path / 'sync_source.db'}"
    store = BookSourceProcessor(path=str(tmp_path), cate1="book", database_url=db_url)
    source_dir = Path(store.path_bok) / "10000000-10000100"
    source_dir.mkdir(parents=True, exist_ok=True)
    source_path = source_dir / "10000001.json"
    source_path.write_text(
        """
        {
          "available": true,
          "merged": [
            {
              "md5_list": ["merged-1", "merged-2"],
              "source": {
                "bookSourceName": "书源A",
                "bookSourceUrl": "https://books.example.com/api/"
              }
            }
          ],
          "candidate": [
            {
              "md5_list": ["candidate-1"],
              "source": {
                "bookSourceName": "书源A增强",
                "bookSourceUrl": "https://books.example.com/api/"
              }
            },
            {
              "md5_list": ["candidate-2", "candidate-3"],
              "source": {
                "bookSourceName": "书源A增强2",
                "bookSourceUrl": "https://books.example.com/api/"
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

    stats = SyncLocalSourceRecordsTask(path=str(tmp_path)).run_book(database_url=db_url)

    detail_records = list_source_detail_records(source_type="book", database_url=db_url)
    index_map = load_source_index_map(source_type="book", database_url=db_url)

    assert stats == {"details": 1, "indexes": 5}
    assert len(detail_records) == 1
    assert detail_records[0].id == 10000001
    assert detail_records[0].url == "books.example.com"
    assert detail_records[0].version == 3
    assert set(index_map.keys()) == {
        "merged-1",
        "merged-2",
        "candidate-1",
        "candidate-2",
        "candidate-3",
    }
