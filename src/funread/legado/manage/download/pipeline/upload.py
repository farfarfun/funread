"""Upload source batches task."""

from nltlog import getLogger

from ..core.constants import EXPORT_BATCH_SIZE
from ..reporting.remote import SourceRemoteManager
from .base import SourcePipelineTask


logger = getLogger("funread")


class UploadSourceBatchesTask(SourcePipelineTask):
    """Upload exported source batches to remote storage."""

    def __init__(self, store=None, remote_manager: SourceRemoteManager = None, *args, **kwargs):
        self.remote_manager = remote_manager
        super(UploadSourceBatchesTask, self).__init__(store=store, *args, **kwargs)

    def run(self) -> None:
        if self.store is None:
            raise ValueError("store is required for UploadSourceBatchesTask")
        if self.remote_manager is None:
            raise ValueError("remote_manager is required for UploadSourceBatchesTask")
        try:
            with self.store as runner:
                self.remote_manager.upload_exported_sources(
                    runner=runner,
                    export_batch_size=EXPORT_BATCH_SIZE,
                )
        except Exception as e:
            logger.error(f"Failed to upload source data: {e}")
            raise
