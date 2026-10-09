import funsecret
import pytest

import funread.base.config as config


def test_database_url_prefers_environment(monkeypatch):
    monkeypatch.setenv("FUNREAD_DATABASE_URL", "sqlite:///environment.db")
    monkeypatch.setattr(funsecret, "read_secret", lambda **_kwargs: "sqlite:///secret.db")

    assert config.resolve_database_url() == "sqlite:///environment.db"


def test_database_url_uses_secret(monkeypatch):
    monkeypatch.delenv("FUNREAD_DATABASE_URL", raising=False)
    monkeypatch.setattr(funsecret, "read_secret", lambda **_kwargs: "sqlite:///secret.db")

    assert config.resolve_database_url() == "sqlite:///secret.db"


def test_database_url_falls_back_to_local_sqlite(monkeypatch, tmp_path):
    database_path = tmp_path / "cache" / "funread.db"
    monkeypatch.delenv("FUNREAD_DATABASE_URL", raising=False)
    monkeypatch.setattr(
        funsecret, "read_secret", lambda **_kwargs: (_ for _ in ()).throw(KeyError())
    )
    monkeypatch.setattr(config, "DEFAULT_DATABASE_PATH", database_path)
    monkeypatch.setattr(config, "DEFAULT_DATABASE_URL", f"sqlite:///{database_path}")

    assert config.resolve_database_url() == f"sqlite:///{database_path}"
    assert database_path.parent.is_dir()


def test_cache_root_prefers_environment(monkeypatch):
    monkeypatch.setenv("FUNREAD_CACHE_ROOT", "/tmp/env-hub")
    monkeypatch.setattr(funsecret, "read_secret", lambda **_kwargs: "/tmp/secret-hub")

    assert config.resolve_cache_root() == "/tmp/env-hub"


def test_cache_root_uses_secret(monkeypatch):
    monkeypatch.delenv("FUNREAD_CACHE_ROOT", raising=False)
    monkeypatch.setattr(funsecret, "read_secret", lambda **_kwargs: "/tmp/secret-hub")

    assert config.resolve_cache_root() == "/tmp/secret-hub"


def test_cache_root_falls_back_without_secret_store(monkeypatch, tmp_path):
    """没配 funsecret 时必须有兜底 —— 以前这里直接抛，API 进程起不来。"""
    cache_root = tmp_path / "cache" / "hub"
    monkeypatch.delenv("FUNREAD_CACHE_ROOT", raising=False)
    monkeypatch.setattr(
        funsecret, "read_secret", lambda **_kwargs: (_ for _ in ()).throw(KeyError("db_url"))
    )
    monkeypatch.setattr(config, "DEFAULT_CACHE_ROOT", cache_root)

    assert config.resolve_cache_root() == str(cache_root)
    assert cache_root.is_dir()


def test_database_url_propagates_secret_system_failure(monkeypatch):
    """secret store 坏掉跟没配是两回事：前者必须响，不能默默跑到本地 SQLite。"""
    monkeypatch.delenv("FUNREAD_DATABASE_URL", raising=False)
    monkeypatch.setattr(
        funsecret, "read_secret", lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("broken"))
    )

    with pytest.raises(RuntimeError, match="broken"):
        config.resolve_database_url()
