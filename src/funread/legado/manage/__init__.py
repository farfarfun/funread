"""Legado manage package."""

from .source_download import (
    SourceDownloadRecord,
    init_source_download_db,
    upsert_source_download_record,
)

__all__ = [
    "SourceDownloadRecord",
    "init_source_download_db",
    "upsert_source_download_record",
]
