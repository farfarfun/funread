"""Restore source data task."""

from typing import Optional

from nltlog import getLogger

from .base import SourcePipelineTask


logger = getLogger("funread")


class RestoreSourceDataTask(SourcePipelineTask):
    """Restore local source data from the latest or a given archive."""

    def __init__(self, store=None, zip_file: Optional[str] = None, *args, **kwargs):
        self.zip_file = zip_file
        super(RestoreSourceDataTask, self).__init__(store=store, *args, **kwargs)

    def run(self) -> None:
        if self.store is None:
            raise ValueError("store is required for RestoreSourceDataTask")
        try:
            with self.store as runner:
                runner.loads_zip(zip_file=self.zip_file)
                logger.info("Source data restored successfully")
        except Exception as e:
            logger.error(f"Failed to restore source data: {e}")
            raise
