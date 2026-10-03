"""回归测试：

1. `download/reporting/builder.py` 里生成的 Legado 导入页订阅源链接，必须
   指向实际会被产出的快照文件（funread/legado/snapshot/lasted/funread.json），
   而不是不存在的 funread/legado/rss/rss-main.json（farfarfun/funread-cache#743
   的根因，详见 CHANGELOG「未发布」小节）。
2. `fundrive`（仅要求 Python >= 3.12）缺失时，`download/context.py`、
   `publish/entrance.py`、`publish/rss.py` 必须仍然可以被正常 import，
   只有在真正构造 `SourceBuildContext`/`UpdateEntrance`/`UpdateRssTask` 时
   才抛出明确的 ImportError（farfarfun/todo-list#742 finding 1）。
"""

import builtins
import importlib
import sys
from contextlib import contextmanager

import pytest

import funread.legado.manage.download.reporting.builder as builder_module

EXPECTED_SNAPSHOT_PATH = "funread/legado/snapshot/lasted/funread.json"
STALE_SNAPSHOT_PATH = "funread/legado/rss/rss-main.json"


def test_rss_sources_point_to_existing_snapshot_file() -> None:
    """RSS_SOURCES 里每个订阅源链接都应指向实际产出的快照文件。"""
    assert builder_module.RSS_SOURCES, "RSS_SOURCES 不应为空"
    for source in builder_module.RSS_SOURCES:
        assert EXPECTED_SNAPSHOT_PATH in source["href"], source
        assert STALE_SNAPSHOT_PATH not in source["href"], source


@contextmanager
def _fundrive_import_blocked():
    """模拟 fundrive 未安装：阻断对 fundrive 及其子模块的 import。"""
    blocked_prefixes = ("fundrive", "fundrive.")
    removed = {
        name: module
        for name, module in list(sys.modules.items())
        if name.startswith(blocked_prefixes)
    }
    for name in removed:
        del sys.modules[name]

    real_import = builtins.__import__

    def _blocking_import(name, *args, **kwargs):
        if name == "fundrive" or name.startswith("fundrive."):
            raise ImportError(f"simulated missing optional dep: {name}")
        return real_import(name, *args, **kwargs)

    builtins.__import__ = _blocking_import
    try:
        yield
    finally:
        builtins.__import__ = real_import
        for name, module in removed.items():
            sys.modules[name] = module


@pytest.mark.parametrize(
    ("module_path", "class_name"),
    [
        ("funread.legado.manage.download.context", "SourceBuildContext"),
        ("funread.legado.manage.publish.entrance", "UpdateEntrance"),
        ("funread.legado.manage.publish.rss", "UpdateRssTask"),
    ],
)
def test_module_imports_cleanly_without_fundrive(module_path: str, class_name: str) -> None:
    """fundrive 缺失时模块本身必须能正常 import，不应拖垮整个包导入。"""
    with _fundrive_import_blocked():
        module = importlib.reload(importlib.import_module(module_path))
        try:
            assert getattr(module, "GithubDrive") is None
        finally:
            # 恢复成正常（已安装 fundrive）状态，避免影响后续用例。
            importlib.reload(module)


@pytest.mark.parametrize(
    "module_path",
    [
        "funread.legado.manage.download.context",
        "funread.legado.manage.publish.entrance",
        "funread.legado.manage.publish.rss",
    ],
)
def test_constructing_without_fundrive_raises_clear_import_error(module_path: str) -> None:
    """fundrive 缺失时，只有在真正构造使用上传能力的类时才应报错。"""
    with _fundrive_import_blocked():
        module = importlib.reload(importlib.import_module(module_path))
        try:
            class_name = {
                "funread.legado.manage.download.context": "SourceBuildContext",
                "funread.legado.manage.publish.entrance": "UpdateEntrance",
                "funread.legado.manage.publish.rss": "UpdateRssTask",
            }[module_path]
            target_cls = getattr(module, class_name)
            with pytest.raises(ImportError, match="funread\\[publish\\]"):
                target_cls()
        finally:
            importlib.reload(module)
