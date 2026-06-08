"""Legado manage package."""

from .source_download import (
    SourceDownloadRecord,
    SourceRecord,
    add_source_download_url,
    add_source_url,
    init_source_download_db,
    iter_source_download_data,
    list_source_records,
    load_source_url_map,
    upsert_source_download_record,
    upsert_source_record,
)

__all__ = [
    "SourceDownloadRecord",
    "SourceRecord",
    "add_source_download_url",
    "add_source_url",
    "init_source_download_db",
    "iter_source_download_data",
    "list_source_records",
    "load_source_url_map",
    "upsert_source_download_record",
    "upsert_source_record",
]
