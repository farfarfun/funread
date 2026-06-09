"""Fetch source data task."""

from nltlog import getLogger

from .base import SourcePipelineTask


logger = getLogger("funread")


class FetchSourceDataTask(SourcePipelineTask):
    """Fetch source lists and write them into local storage."""

    def run(self) -> None:
        if self.store is None:
            raise ValueError("store is required for FetchSourceDataTask")
        try:
            with self.store as runner:
                runner.loader()
                logger.info("Source data loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load source data: {e}")
            raise
