"""全局测试夹具。

本机 funsecret 里 `funread/cache/source/db_url` 指向**生产 MySQL**，任何忘了覆盖
`FUNREAD_DATABASE_URL` 的测试都会写生产库。这里用 autouse fixture 强制覆盖成临时
SQLite，挡住整类事故。
"""

import os

import pytest


@pytest.fixture(autouse=True)
def _isolate_database(tmp_path, monkeypatch):
    monkeypatch.setenv("FUNREAD_DATABASE_URL", f"sqlite:///{tmp_path / 'funread-test.db'}")
    monkeypatch.setenv("FUNREAD_CACHE_ROOT", str(tmp_path / "hubs"))
    yield


@pytest.fixture
def fixtures_dir():
    return os.path.join(os.path.dirname(__file__), "fixtures")
