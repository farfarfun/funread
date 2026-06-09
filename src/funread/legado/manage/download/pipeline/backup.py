"""Backup source data task."""

from nltlog import getLogger

from .base import SourcePipelineTask


logger = getLogger("funread")


class BackupSourceDataTask(SourcePipelineTask):
    """Compress local source data into a backup archive."""

    def run(self) -> str:
        if self.store is None:
            raise ValueError("store is required for BackupSourceDataTask")
        try:
            with self.store as runner:
                zip_path = runner.dumps_zip()
                logger.info(f"Source data compressed successfully: {zip_path}")
                return zip_path
        except Exception as e:
            logger.error(f"Failed to compress source data: {e}")
            raise
