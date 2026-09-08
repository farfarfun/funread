"""Back up the live SQLite database file itself into a destination directory
(e.g. a funread-dat working copy) — a plain file copy, not a tar.xz snapshot,
so it can be inspected/restored by just pointing sqlite3 at it.

No-ops (and returns ``None``) when the resolved database isn't SQLite:
a remote MySQL has its own backup story, there's no local file to copy.
"""

import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional

from farlog import getLogger

from funread.base.config import resolve_database_url, sqlite_path

logger = getLogger("funread")


def backup_sqlite_database(dest_dir: str) -> Optional[str]:
    database_url = resolve_database_url()
    source_path = sqlite_path(database_url)
    if source_path is None:
        logger.info(f"Database is not a local SQLite file ({database_url.split('://', 1)[0]}); skip backup")
        return None
    if not source_path.exists():
        logger.warning(f"SQLite database file not found: {source_path}")
        return None

    dest_dir_path = Path(dest_dir)
    dest_dir_path.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    dest_path = dest_dir_path / f"funread-{timestamp}.db"

    shutil.copy2(source_path, dest_path)
    logger.info(f"Backed up SQLite database {source_path} -> {dest_path}")
    return str(dest_path)
