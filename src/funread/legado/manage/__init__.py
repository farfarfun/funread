"""Legado manage package."""

from .publish import UpdateEntrance, UpdateRssTask
from .source import (
    SourceDetailRecord,
    SourceIndexRecord,
    SourceListRecord,
    SyncLocalSourceRecordsTask,
    add_source_detail_url,
    add_source_list_url,
    init_source_db,
    iter_source_list_data,
    list_source_detail_records,
    load_source_index_map,
    load_source_detail_url_map,
    replace_source_detail_records,
    replace_source_index_records,
    upsert_source_index_records,
    upsert_source_detail_record,
    upsert_source_list_record,
)

__all__ = [
    "SourceDetailRecord",
    "SourceIndexRecord",
    "SourceListRecord",
    "SyncLocalSourceRecordsTask",
    "UpdateEntrance",
    "UpdateRssTask",
    "add_source_detail_url",
    "add_source_list_url",
    "init_source_db",
    "iter_source_list_data",
    "list_source_detail_records",
    "load_source_index_map",
    "load_source_detail_url_map",
    "replace_source_detail_records",
    "replace_source_index_records",
    "upsert_source_index_records",
    "upsert_source_detail_record",
    "upsert_source_list_record",
]
