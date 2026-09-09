import funsecret

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
        funsecret, "read_secret", lambda **_kwargs: (_ for _ in ()).throw(RuntimeError())
    )
    monkeypatch.setattr(config, "DEFAULT_DATABASE_PATH", database_path)
    monkeypatch.setattr(config, "DEFAULT_DATABASE_URL", f"sqlite:///{database_path}")

    assert config.resolve_database_url() == f"sqlite:///{database_path}"
    assert database_path.parent.is_dir()
