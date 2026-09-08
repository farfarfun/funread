"""Database URL resolution.

Priority: ``FUNREAD_DATABASE_URL`` env var > funsecret
(``funread/cache/source/db_url``) > local SQLite fallback. This mirrors
funflix's ``base/config.py`` so a fresh clone can run the pipeline and API
against a local SQLite file without any secret store configured.
"""

from __future__ import annotations

import os
from pathlib import Path

from farlog import getLogger

logger = getLogger("funread")

#: Fixed under the user cache dir (not CWD-relative) so every entrypoint
#: (CLI pipeline, API server, scripts) reads/writes the same file regardless
#: of where it's invoked from.
DEFAULT_DATABASE_PATH = Path.home() / ".cache" / "farfarfun" / "funread" / "funread.db"
DEFAULT_DATABASE_URL = f"sqlite:///{DEFAULT_DATABASE_PATH}"


def _fallback_to_default() -> str:
    DEFAULT_DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    return DEFAULT_DATABASE_URL


def resolve_database_url() -> str:
    """Resolve the database URL to use, falling back to local SQLite.

    Never raises: funsecret being unconfigured or unavailable falls back to
    ``DEFAULT_DATABASE_URL`` instead of failing the caller.
    """
    env_value = os.environ.get("FUNREAD_DATABASE_URL")
    if env_value:
        return env_value

    try:
        from funsecret import read_secret

        value = read_secret(cate1="funread", cate2="cache", cate3="source", cate4="db_url")
    except Exception as exc:
        logger.debug(f"Failed to read funread/cache/source/db_url from funsecret: {exc}")
        value = None

    if value:
        return value

    return _fallback_to_default()


def is_sqlite_url(database_url: str) -> bool:
    return database_url.startswith("sqlite")


def sqlite_path(database_url: str) -> Path | None:
    """Extract the filesystem path from a ``sqlite:///...`` URL, or ``None``."""
    if not is_sqlite_url(database_url) or ":memory:" in database_url:
        return None
    _, _, path = database_url.partition("sqlite:///")
    return Path(path) if path else None
