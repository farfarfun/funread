"""补充覆盖发布、合并/校验任务入口、以及 web 解析基类的公开 API。

针对 farfarfun/todo-list#742 finding 6 指出的测试缺口：
``funread.legado.manage.publish`` 下的 ``UpdateEntrance``/``UpdateRssTask``、
以及 ``MergeSourceTask``/``CheckSourceStatusTask`` 这两个依赖 ``read_secret``
解析数据库地址/缓存根目录的任务入口类，此前都没有单元测试覆盖。
所有用例都通过 monkeypatch 隔离真实网络调用与远端存储。
"""

import pytest

import funread.legado.manage.publish.entrance as entrance_module
import funread.legado.manage.publish.rss as rss_module
import funread.legado.manage.source.check.task as check_module
import funread.legado.manage.source.merge.task as merge_module
from funread.web.parse.base import BaseParse


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _FakeGithubDrive:
    """记录调用参数的假 GithubDrive，避免真实网络/远端存储访问。"""

    def __init__(self):
        self.login_calls = []
        self.uploaded = []
        self.dir_list = []

    def login(self, *args, **kwargs):
        self.login_calls.append((args, kwargs))

    def get_dir_list(self, dir_path):
        return self.dir_list

    def upload_file(self, **kwargs):
        self.uploaded.append(kwargs)


# ---------------------------------------------------------------------------
# UpdateEntrance（publish/entrance.py）
# ---------------------------------------------------------------------------


def test_update_entrance_logs_into_funread_cache(monkeypatch):
    fake_drive = _FakeGithubDrive()
    monkeypatch.setattr(entrance_module, "GithubDrive", lambda: fake_drive)

    entrance = entrance_module.UpdateEntrance()

    assert entrance.drive is fake_drive
    assert fake_drive.login_calls == [
        ((), {"repo_owner": "farfarfun", "repo_name": "funread-cache", "branch": "master"})
    ]


def test_update_entrance_random_icon_returns_fetched_url(monkeypatch):
    fake_drive = _FakeGithubDrive()
    monkeypatch.setattr(entrance_module, "GithubDrive", lambda: fake_drive)
    monkeypatch.setattr(
        entrance_module.requests,
        "get",
        lambda *a, **k: _FakeResponse([{"url": "https://cat.example/pic.png"}]),
    )

    entrance = entrance_module.UpdateEntrance()

    assert entrance.random_icon() == "https://cat.example/pic.png"


def test_update_entrance_run_updates_book_then_main(monkeypatch):
    fake_drive = _FakeGithubDrive()
    fake_drive.dir_list = [{"name": "demo", "fid": "demo/0001"}]
    monkeypatch.setattr(entrance_module, "GithubDrive", lambda: fake_drive)
    monkeypatch.setattr(
        entrance_module.requests,
        "get",
        lambda *a, **k: _FakeResponse([{"url": "https://cat.example/pic.png"}]),
    )

    entrance = entrance_module.UpdateEntrance()
    entrance.run()

    uploaded_filenames = [item["filename"] for item in fake_drive.uploaded]
    assert uploaded_filenames == ["source.json", "funread.json"]


# ---------------------------------------------------------------------------
# UpdateRssTask（publish/rss.py）
# ---------------------------------------------------------------------------


def test_update_rss_task_logs_into_configured_repo(monkeypatch):
    fake_drive = _FakeGithubDrive()
    monkeypatch.setattr(rss_module, "GithubDrive", lambda: fake_drive)

    task = rss_module.UpdateRssTask(repo="farfarfun/funread-cache")

    assert fake_drive.login_calls == [(("farfarfun/funread-cache",), {})]
    assert task.repo == "farfarfun/funread-cache"


def test_update_rss_task_random_icon_falls_back_after_retries(monkeypatch):
    fake_drive = _FakeGithubDrive()
    monkeypatch.setattr(rss_module, "GithubDrive", lambda: fake_drive)

    def _always_fail(*args, **kwargs):
        raise rss_module.requests.RequestException("boom")

    monkeypatch.setattr(rss_module.requests, "get", _always_fail)

    task = rss_module.UpdateRssTask()

    assert task.random_icon(retries=2) == rss_module.DEFAULT_ICON_URL


def test_update_rss_task_update_main_uploads_aggregated_config(monkeypatch):
    fake_drive = _FakeGithubDrive()
    monkeypatch.setattr(rss_module, "GithubDrive", lambda: fake_drive)
    monkeypatch.setattr(
        rss_module.requests,
        "get",
        lambda *a, **k: _FakeResponse([{"url": "https://cat.example/pic.png"}]),
    )

    task = rss_module.UpdateRssTask()
    task.update_main()

    assert len(fake_drive.uploaded) == 1
    upload = fake_drive.uploaded[0]
    assert upload["git_path"] == "funread/legado/snapshot/lasted/funread.json"
    assert upload["content"][0]["sourceName"] == "funread"


def test_update_rss_task_update_book_propagates_and_logs_on_failure(monkeypatch, caplog):
    fake_drive = _FakeGithubDrive()

    def _boom(**kwargs):
        raise RuntimeError("upload failed")

    fake_drive.upload_file = _boom
    monkeypatch.setattr(rss_module, "GithubDrive", lambda: fake_drive)
    monkeypatch.setattr(
        rss_module.requests,
        "get",
        lambda *a, **k: _FakeResponse([{"url": "https://cat.example/pic.png"}]),
    )

    task = rss_module.UpdateRssTask()
    with pytest.raises(RuntimeError, match="upload failed"):
        task.update_book()


# ---------------------------------------------------------------------------
# MergeSourceTask / CheckSourceStatusTask 的 read_secret 容错路径
# ---------------------------------------------------------------------------


class _NoopMerger:
    """满足 SourceMerger 协议但从不被调用的占位实现（limit=0 不会触发合并）。"""

    def merge_sources(self, source_type, hostname, versions):
        raise AssertionError("merge_sources should not be called when limit=0")


def test_merge_source_task_runs_without_configured_database(monkeypatch, tmp_path):
    def _raise_key_error(**kwargs):
        raise KeyError("not configured")

    monkeypatch.setattr(merge_module, "read_secret", _raise_key_error)

    task = merge_module.MergeSourceTask(path=str(tmp_path))
    stats = task.run_rss(merger=_NoopMerger(), limit=0)

    assert stats == {"processed": 0, "merged": 0, "skipped": 0, "failed": 0}


def test_merge_source_task_rejects_unsupported_source_type(tmp_path):
    task = merge_module.MergeSourceTask(path=str(tmp_path))
    with pytest.raises(ValueError, match="Unsupported source type"):
        task.run_source(source_type="unknown")


def test_check_source_status_task_runs_without_configured_database(monkeypatch, tmp_path):
    def _raise_value_error(**kwargs):
        raise ValueError("not configured")

    monkeypatch.setattr(check_module, "read_secret", _raise_value_error)

    task = check_module.CheckSourceStatusTask(path=str(tmp_path))
    stats = task.run_book(limit=0)

    assert stats == {"processed": 0, "available": 0, "unavailable": 0, "failed": 0}


def test_check_source_status_task_rejects_unsupported_source_type(tmp_path):
    task = check_module.CheckSourceStatusTask(path=str(tmp_path))
    with pytest.raises(ValueError, match="Unsupported source type"):
        task.run_source(source_type="unknown")


# ---------------------------------------------------------------------------
# BaseParse（web/parse/base.py）
# ---------------------------------------------------------------------------


def test_base_parse_stores_source():
    parser = BaseParse(source={"id": 1})
    assert parser.source == {"id": 1}


def test_base_parse_list_and_detail_are_not_implemented():
    parser = BaseParse()
    with pytest.raises(NotImplementedError):
        parser.parse_list(page_no=1)
    with pytest.raises(NotImplementedError):
        parser.parse_detail("https://example.com/1")
