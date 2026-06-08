"""Legado manage package."""

from .source_download import (
    SourceDetailRecord,
    SourceListRecord,
    add_source_detail_url,
    add_source_list_url,
    init_source_db,
    iter_source_list_data,
    list_source_detail_records,
    load_source_detail_url_map,
    upsert_source_detail_record,
    upsert_source_list_record,
)

__all__ = [
    "SourceDetailRecord",
    "SourceListRecord",
    "add_source_detail_url",
    "add_source_list_url",
    "init_source_db",
    "iter_source_list_data",
    "list_source_detail_records",
    "load_source_detail_url_map",
    "upsert_source_detail_record",
    "upsert_source_list_record",
]
