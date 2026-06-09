"""Remote publishing helpers for source snapshots."""

from typing import Any, Dict, List
import json
import re

from nltlog import getLogger
from nlttask import Task

from ..core.constants import EXPORT_BATCH_SIZE


logger = getLogger("funread")


class SourceRemoteManager:
    """Upload split source batches and publish generated report files."""

    def __init__(self, context: Any, initial_counter: int, min_upload_batch_size: int):
        self.context = context
        self.initial_counter = initial_counter
        self.min_upload_batch_size = min_upload_batch_size

    @staticmethod
    def is_file_too_large_error(error: Exception) -> bool:
        message = str(error).lower()
        return "too large" in message or "422" in message

    def upload_single_batch(self, data: List[Dict[str, Any]], counter: int) -> None:
        git_path = f"{self.context.dir_path}/progress-{counter}.json"
        filename = f"progress-{counter}.json"
        self.context.drive.upload_file(
            content=json.dumps(data),
            fid=self.context.dir_path,
            filepath=None,
            filename=filename,
        )
        self.context._remember_source_count(git_path, filename, count=len(data))
        logger.info(f"Uploaded {len(data)} sources to {git_path}")

    def upload_batch(self, data: List[Dict[str, Any]], counter: int) -> int:
        try:
            self.upload_single_batch(data, counter)
            return counter + 1
        except Exception as e:
            if self.is_file_too_large_error(e) and len(data) > self.min_upload_batch_size:
                split_size = max(len(data) // 2, self.min_upload_batch_size)
                logger.warning(
                    f"Batch {counter} too large with {len(data)} sources, split into chunks of {split_size}"
                )
                next_counter = counter
                for start in range(0, len(data), split_size):
                    next_counter = self.upload_batch(data[start : start + split_size], next_counter)
                return next_counter
            logger.error(f"Failed to upload batch {counter}: {e}")
            raise

    def cleanup_stale_remote_batches(self, next_counter: int) -> None:
        try:
            files = self.context.drive.get_file_list(self.context.dir_path)
            stale_files = []
            for file in files:
                match = re.fullmatch(r"progress-(\d+)\.json", str(file.name))
                if not match:
                    continue
                counter = int(match.group(1))
                if counter >= next_counter:
                    stale_files.append((counter, file.fid))
            for counter, fid in sorted(stale_files):
                if self.context.drive.delete(fid):
                    logger.info(f"Deleted stale remote batch progress-{counter}.json")
                else:
                    logger.warning(f"Failed to delete stale remote batch progress-{counter}.json")
        except Exception as e:
            logger.warning(f"Failed to cleanup stale remote batches: {e}")

    def publish_html_report(self, html_content: str) -> None:
        self.context.drive.upload_file(
            filepath=None,
            fid=f"{self.context.dir_path}",
            filename="index.html",
            content=html_content,
        )
        logger.info("RSS configuration updated successfully")

    def upload_exported_sources(self, runner: Any, export_batch_size: int) -> None:
        counter = self.initial_counter
        uploaded_any = False
        for data in runner.export_sources(size=export_batch_size):
            if data:
                uploaded_any = True
                counter = self.upload_batch(data, counter)
        if uploaded_any:
            self.cleanup_stale_remote_batches(counter)
        else:
            logger.warning("No exported source batches produced; skip remote cleanup")


class UploadSourceBatchesTask(Task):
    """Upload exported source batches to remote storage."""

    def __init__(self, store=None, remote_manager: SourceRemoteManager = None, *args, **kwargs):
        self.store = store
        self.remote_manager = remote_manager
        super(UploadSourceBatchesTask, self).__init__(*args, **kwargs)

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


class PublishSourceReportTask(Task):
    """Generate and publish the HTML report for a source directory."""

    def __init__(
        self,
        report_builder=None,
        remote_manager: SourceRemoteManager = None,
        *args,
        **kwargs,
    ):
        self.report_builder = report_builder
        self.remote_manager = remote_manager
        super(PublishSourceReportTask, self).__init__(*args, **kwargs)

    def run(self) -> None:
        if self.report_builder is None:
            raise ValueError("report_builder is required for PublishSourceReportTask")
        if self.remote_manager is None:
            raise ValueError("remote_manager is required for PublishSourceReportTask")
        try:
            html_content = self.report_builder.generate_html_report()
            self.remote_manager.publish_html_report(html_content)
        except Exception as e:
            logger.error(f"Failed to update RSS: {e}")
            raise
