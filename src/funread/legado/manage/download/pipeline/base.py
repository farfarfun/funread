"""Base task types for the source pipeline."""

from typing import Optional

from nlttask import Task

from ..core.store import LocalSourceStore


class SourcePipelineTask(Task):
    """Base class for tasks that operate on a local source store."""

    def __init__(self, store: Optional[LocalSourceStore] = None, *args, **kwargs):
        self.store = store
        super(SourcePipelineTask, self).__init__(*args, **kwargs)
