"""三个任务入口必须从配置链拿库地址，不许直读 funsecret。

check / merge / sync(管线) 这三处各自建自己的 store，所以各自都得解析一次库地址。
历史上它们直读过 `funsecret`，而本机密钥库里是**生产 PostgreSQL** —— 那个版本在本地
跑一次探活就会往生产库写。修回来之后，防线一度只有源码里的一句注释，而注释在合并
冲突里很容易被当成风格差异丢掉（`docs/retrospective/reader-client/001-overview.md`
问题一记的就是这件事真的发生过一次）。

所以这里对每个入口断言同一件事：**进程内 override 里的那个地址必须出现在它建的
store 上**。谁再把哪一处改回直读密钥库，捕到的地址就不是 override 的那个，这三条
立刻红。
"""

import contextlib

import funread.legado.manage.download.task as generate_task_module
import funread.legado.manage.source.check.task as check_module
import funread.legado.manage.source.merge.task as merge_module
from funread.base import config

#: 一个不可能从别处冒出来的地址。用 sqlite 是因为万一哪天被真的连上去，也只是在
#: tmp 下建个文件，而不是打到某台真实数据库上。
SENTINEL_URL = "sqlite:////tmp/funread-chain-sentinel.db"


class _Store:
    """占位 store。两个 runner 都被换掉了，所以它不需要有任何行为。"""


def _capturing_store(captured: list[str | None]):
    """替掉 `_create_store`：记下传进来的 database_url，返回一个空 store。"""

    #  装在类上，所以第一个参数是 self。
    @contextlib.contextmanager
    def _create_store(self, path, *, source_type, database_url=None):
        captured.append(database_url)
        yield _Store()

    return _create_store


class _NoopRunner:
    """两个 runner 的共同替身：构造参数随便收，`run` 什么也不做。"""

    def __init__(self, *args, **kwargs):
        pass

    def run(self, limit=None):
        return {}


def test_check_source_status_task_takes_the_database_url_from_the_chain(monkeypatch, tmp_path):
    captured: list[str | None] = []
    monkeypatch.setattr(
        check_module.CheckSourceStatusTask, "_create_store", _capturing_store(captured)
    )
    monkeypatch.setattr(check_module, "SourceStatusCheckRunner", _NoopRunner)

    task = check_module.CheckSourceStatusTask(path=str(tmp_path))
    with config.override(database_url=SENTINEL_URL):
        task.run_book(limit=0)

    assert captured == [SENTINEL_URL]


def test_merge_source_task_takes_the_database_url_from_the_chain(monkeypatch, tmp_path):
    captured: list[str | None] = []
    monkeypatch.setattr(merge_module.MergeSourceTask, "_create_store", _capturing_store(captured))
    monkeypatch.setattr(merge_module, "SourceMergeRunner", _NoopRunner)

    task = merge_module.MergeSourceTask(path=str(tmp_path))
    with config.override(database_url=SENTINEL_URL):
        task.run_rss(limit=0)

    assert captured == [SENTINEL_URL]


def test_the_pipeline_takes_the_database_url_from_the_chain(monkeypatch, tmp_path):
    """管线（sync 那一段）把地址挂在 store 上传给下游，所以断言落在 store 的属性上。"""

    class _Context:
        def create_store(self, path):
            return _Store()

    monkeypatch.setattr(
        generate_task_module.GenerateSourceTask,
        "get_cache_root",
        staticmethod(lambda: str(tmp_path)),
    )
    monkeypatch.setattr(
        generate_task_module.GenerateSourceTask,
        "build_context",
        lambda self, source_type: _Context(),
    )

    with config.override(database_url=SENTINEL_URL):
        runtime = generate_task_module.GenerateSourceTask().build_runtime("booksource")

    assert runtime["store"].database_url == SENTINEL_URL
