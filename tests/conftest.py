"""全局测试夹具。

本机 funsecret 里 `funread/cache/source/db_url` 指向**生产 PostgreSQL**，任何忘了
隔离的测试都会写生产库。这里用 autouse fixture 把库地址和缓存目录强制覆盖成临时
路径，挡住整类事故。

注意这里用的是**进程内 override**，不是 `monkeypatch.setenv`。`FUNREAD_*` 环境变量
那一层已经整个删掉了（见 `funread.base.config` 的模块文档），继续 setenv 的话这个
fixture 会变成空操作，然后一路掉到 funsecret 上去 —— 也就是生产库。
`test_config.py::test_environment_variables_are_ignored` 专门钉住这件事。

`shared_config_path()` 也要指到临时位置：它默认是 `~/.farfarfun/funread/config.toml`，
开发机上那份是真的，不挡住的话测试会读到真机配置。
"""

import os

import pytest

from funread.base import config


@pytest.fixture(autouse=True)
def _isolate_database(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "shared_config_path", lambda: tmp_path / "shared-config.toml")
    config.reset_config_files()
    config.set_overrides(
        database_url=f"sqlite:///{tmp_path / 'funread-test.db'}",
        cache_root=str(tmp_path / "hubs"),
    )
    yield
    config.clear_overrides()
    config.reset_config_files()


@pytest.fixture
def fixtures_dir():
    return os.path.join(os.path.dirname(__file__), "fixtures")


def pytest_addoption(parser):
    parser.addoption(
        "--fail-on-skip",
        action="store_true",
        default=False,
        help="有任何用例被 skip 就让整次运行失败。CI 用：按 extra 装的依赖缺了，对应用例会"
        " skip 而不是报错，绿得毫无意义（2026-10-17 发现 29 个用例在静默 skip）。",
    )


def pytest_sessionfinish(session, exitstatus):
    if not session.config.getoption("--fail-on-skip"):
        return
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    skipped = reporter.stats.get("skipped", []) if reporter else []
    if skipped:
        reporter.write_line(f"--fail-on-skip：{len(skipped)} 个用例被 skip，按失败处理", red=True)
        session.exitstatus = 1
