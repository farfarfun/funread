"""Database URL and cache root resolution.

Priority: ``FUNREAD_DATABASE_URL`` env var > funsecret
(``funread/cache/source/db_url``) > local SQLite fallback. This mirrors
funflix's ``base/config.py`` so a fresh clone can run the pipeline and API
against a local SQLite file without any secret store configured.

The cache root (where source JSON files live) follows the same shape:
``FUNREAD_CACHE_ROOT`` > funsecret (``funread/cache/path/root``) > a fixed
path under the user cache dir.
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

#: Where downloaded source JSON files are archived. Same reasoning as the
#: database path: fixed under the user cache dir, never CWD-relative.
DEFAULT_CACHE_ROOT = Path.home() / ".cache" / "farfarfun" / "funread" / "hub"


def _fallback_to_default() -> str:
    DEFAULT_DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    return DEFAULT_DATABASE_URL


#: Errors that mean "no secret configured here", as opposed to "the secret
#: store malfunctioned". Only the former falls back -- a broken store must stay
#: loud, otherwise a misconfigured host silently writes to local SQLite while
#: the operator believes it is on the configured database.
_SECRET_ABSENT = (ImportError, KeyError, ValueError)


def _read_secret(*, cate3: str, cate4: str) -> str | None:
    try:
        from funsecret import read_secret

        return read_secret(cate1="funread", cate2="cache", cate3=cate3, cate4=cate4)
    except _SECRET_ABSENT as exc:
        logger.debug(f"funread/cache/{cate3}/{cate4} not configured in funsecret: {exc}")
        return None


def resolve_database_url() -> str:
    """Resolve the database URL to use, falling back to local SQLite.

    funsecret being *unconfigured* falls back to ``DEFAULT_DATABASE_URL``
    instead of failing the caller. A secret store that *malfunctions* raises --
    silently falling back would hide which database the process is writing to.
    """
    env_value = os.environ.get("FUNREAD_DATABASE_URL")
    if env_value:
        return env_value

    value = _read_secret(cate3="source", cate4="db_url")
    if value:
        return value

    return _fallback_to_default()


def resolve_cache_root() -> str:
    """Resolve the source-file cache root, falling back to the user cache dir.

    Falls back when funsecret is unconfigured. The pipeline tasks used to read
    funsecret directly with no fallback, which made them unusable from a
    process that has no secret store configured (the API server, a fresh
    clone, CI). A malfunctioning store raises, same as
    :func:`resolve_database_url`.
    """
    env_value = os.environ.get("FUNREAD_CACHE_ROOT")
    if env_value:
        return env_value

    value = _read_secret(cate3="path", cate4="root")
    if value:
        return str(value)

    DEFAULT_CACHE_ROOT.mkdir(parents=True, exist_ok=True)
    return str(DEFAULT_CACHE_ROOT)


def is_sqlite_url(database_url: str) -> bool:
    return database_url.startswith("sqlite")


def sqlite_path(database_url: str) -> Path | None:
    """Extract the filesystem path from a ``sqlite:///...`` URL, or ``None``."""
    if not is_sqlite_url(database_url) or ":memory:" in database_url:
        return None
    _, _, path = database_url.partition("sqlite:///")
    return Path(path) if path else None
