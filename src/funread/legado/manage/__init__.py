"""Legado manage package."""

from .source_download import (
    SourceDownloadRecord,
    add_source_download_url,
    init_source_download_db,
    iter_source_download_data,
    upsert_source_download_record,
)

__all__ = [
    "SourceDownloadRecord",
    "add_source_download_url",
    "init_source_download_db",
    "iter_source_download_data",
    "upsert_source_download_record",
]
