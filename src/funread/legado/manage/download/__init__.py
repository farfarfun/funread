"""Source download package."""

from importlib import import_module

from .context import SourceBuildContext
from .core import (
    DEFAULT_BACKUP_HOST,
    DEFAULT_BACKUP_ID,
    DEFAULT_DIR_PATH,
    DEFAULT_REPO,
    EXPORT_BATCH_SIZE,
    INITIAL_COUNTER,
    MAX_PICKLE_SIZE,
    MIN_UPLOAD_BATCH_SIZE,
    REQUEST_TIMEOUT,
    DownloadSourceDataTask,
    DumpSourceBackupTask,
    LoadSourceBackupTask,
    LocalSourceStore,
    SourceProcessor,
    SourceStoreTask,
)
from .reporting import PublishSourceReportTask, UploadSourceBatchesTask
from .sources import (
    BookSourceFormat,
    BookSourceProcessor,
    RSSSourceFormat,
    RSSSourceProcessor,
    SourceStoreFactory,
)

__all__ = [
    "BookSourceFormat",
    "BookSourceProcessor",
    "DEFAULT_BACKUP_HOST",
    "DEFAULT_BACKUP_ID",
    "DEFAULT_DIR_PATH",
    "DEFAULT_REPO",
    "DownloadSourceDataTask",
    "DumpSourceBackupTask",
    "EXPORT_BATCH_SIZE",
    "GenerateSourceTask",
    "INITIAL_COUNTER",
    "LoadSourceBackupTask",
    "LocalSourceStore",
    "MAX_PICKLE_SIZE",
    "MIN_UPLOAD_BATCH_SIZE",
    "PublishSourceReportTask",
    "RSSSourceFormat",
    "RSSSourceProcessor",
    "REQUEST_TIMEOUT",
    "SourceBuildContext",
    "SourceProcessor",
    "SourceStoreTask",
    "SourceStoreFactory",
    "UploadSourceBatchesTask",
]


def __getattr__(name):
    if name == "GenerateSourceTask":
        return import_module(".task", __name__).GenerateSourceTask
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
