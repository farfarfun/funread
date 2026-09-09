import sqlite3

import funread.legado.manage.download.core.db_backup as db_backup


def test_backup_sqlite_database_includes_wal_data(monkeypatch, tmp_path):
    source_path = tmp_path / "source.db"
    source = sqlite3.connect(source_path)
    source.execute("PRAGMA journal_mode=WAL")
    source.execute("CREATE TABLE sources (name TEXT)")
    source.execute("INSERT INTO sources VALUES ('rss')")
    source.commit()
    monkeypatch.setattr(db_backup, "resolve_database_url", lambda: f"sqlite:///{source_path}")

    backup_path = db_backup.backup_sqlite_database(str(tmp_path / "backups"))

    assert backup_path is not None
    with sqlite3.connect(backup_path) as backup:
        assert backup.execute("SELECT name FROM sources").fetchone() == ("rss",)
    source.close()


def test_backup_sqlite_database_skips_remote_database(monkeypatch, tmp_path):
    destination = tmp_path / "backups"
    monkeypatch.setattr(db_backup, "resolve_database_url", lambda: "mysql+pymysql://host/funread")

    assert db_backup.backup_sqlite_database(str(destination)) is None
    assert not destination.exists()
