"""Runtime context for source download tasks."""

from typing import Any, Dict, List

from fundrive.drives.github import GithubDrive

from .core.constants import DEFAULT_REPO, INITIAL_COUNTER, MIN_UPLOAD_BATCH_SIZE
from .reporting import SourceRemoteManager, SourceReportBuilder
from .sources import SourceStoreFactory


class SourceBuildContext:
    """Shared runtime context for source generation tasks."""

    def __init__(
        self,
        dir_path: str = "funread/legado/book/snapshot/20231011",
        source_type: str = "booksource",
        repo: str = DEFAULT_REPO,
    ):
        self.repo_str = repo
        self.dir_path = dir_path
        self.source_type = source_type
        self.drive = GithubDrive()
        self._source_count_cache: Dict[str, str] = {}
        self.report_builder = SourceReportBuilder(self)
        self.remote_manager = SourceRemoteManager(
            context=self,
            initial_counter=INITIAL_COUNTER,
            min_upload_batch_size=MIN_UPLOAD_BATCH_SIZE,
        )
        self.drive.login(
            repo_owner=self.repo_str.split("/")[0],
            repo_name=self.repo_str.split("/")[1],
            branch="master",
        )

    def _remember_source_count(self, *keys: Any, count: Any) -> None:
        count_text = str(count)
        for key in keys:
            if key:
                self._source_count_cache[str(key)] = count_text

    def create_store(self, path: str):
        return SourceStoreFactory.create(path=path, source_type=self.source_type)

    def format_file_size(self, size: Any) -> str:
        return self.report_builder.format_file_size(size)

    def extract_source_count(self, file: Dict[str, Any]) -> str:
        return self.report_builder.extract_source_count(file)

    def generate_table(self) -> None:
        self.report_builder.generate_table()

    def generate_html_report(self) -> str:
        return self.report_builder.generate_html_report()

    def upload_single_batch(self, data: List[Dict[str, Any]], counter: int) -> None:
        self.remote_manager.upload_single_batch(data, counter)

    def upload_batch(self, data: List[Dict[str, Any]], counter: int) -> int:
        return self.remote_manager.upload_batch(data, counter)

    def cleanup_stale_remote_batches(self, next_counter: int) -> None:
        self.remote_manager.cleanup_stale_remote_batches(next_counter)
