"""Source generation orchestration task."""

from typing import Any, Dict

from funsecret import read_secret
from nltlog import getLogger
from nlttask import Task

from ..source.merge.task import SourceMergeRunner
from ..source.sync.task import SyncLocalSourceRecordsTask
from .context import SourceBuildContext
from .core.constants import DEFAULT_DIR_PATH, DEFAULT_REPO
from .core.store import DownloadSourceDataTask, DumpSourceBackupTask, LoadSourceBackupTask
from .reporting.remote import PublishSourceReportTask, UploadSourceBatchesTask


logger = getLogger("funread")


class GenerateSourceTask(Task):
    """Run the full source build pipeline for a given source type."""

    def __init__(
        self,
        dir_path: str = DEFAULT_DIR_PATH,
        source_type: str = "booksource",
        repo: str = DEFAULT_REPO,
        *args,
        **kwargs,
    ):
        self.repo_str = repo
        self.dir_path = dir_path
        self.source_type = source_type
        super(GenerateSourceTask, self).__init__(*args, **kwargs)

    @staticmethod
    def get_cache_root() -> str:
        return read_secret(cate1="funread", cate2="cache", cate3="path", cate4="root")

    def build_context(self, source_type: str) -> SourceBuildContext:
        return SourceBuildContext(
            source_type=source_type,
            dir_path=f"{self.dir_path}/{'book' if source_type == 'booksource' else 'rss'}",
            repo=self.repo_str,
        )

    def build_runtime(self, source_type: str) -> Dict[str, Any]:
        context = self.build_context(source_type)
        path = self.get_cache_root()
        return {
            "path": path,
            "source_type": source_type,
            "context": context,
            "store": context.create_store(path),
        }

    def run_pipeline(
        self,
        source_type: str,
        load: bool = False,
        download: bool = False,
        merge: bool = False,
        dump: bool = False,
        sync: bool = False,
        upload: bool = False,
        publish: bool = False,
        *args,
        **kwargs,
    ) -> Dict[str, Any]:
        runtime = self.build_runtime(source_type)
        context = runtime["context"]
        store = runtime["store"]

        if load:
            LoadSourceBackupTask(store=store).run()
        if download:
            DownloadSourceDataTask(store=store).run()
        if merge:
            SourceMergeRunner(store=store).run()
        if dump:
            DumpSourceBackupTask(store=store).run()
        if sync:
            SyncLocalSourceRecordsTask(path=runtime["path"]).run_source(
                source_type="book" if source_type == "booksource" else "rss",
                database_url=store.database_url,
            )
        if upload:
            UploadSourceBatchesTask(store=store, remote_manager=context.remote_manager).run()
        if publish:
            PublishSourceReportTask(
                report_builder=context.report_builder,
                remote_manager=context.remote_manager,
            ).run()
        return runtime

    def run_book(
        self,
        load: bool = False,
        download: bool = False,
        merge: bool = False,
        dump: bool = False,
        sync: bool = False,
        upload: bool = False,
        publish: bool = False,
        *args,
        **kwargs,
    ) -> None:
        try:
            logger.info("Starting book source generation task")
            self.run_pipeline(
                "booksource",
                load=load,
                download=download,
                merge=merge,
                dump=dump,
                sync=sync,
                upload=upload,
                publish=publish,
                *args,
                **kwargs,
            )
            logger.info("Book source generation task completed successfully")
        except Exception as e:
            logger.error(f"Book source generation failed: {e}")
            raise

    def run_rss(
        self,
        load: bool = False,
        download: bool = False,
        merge: bool = False,
        dump: bool = False,
        sync: bool = False,
        upload: bool = False,
        publish: bool = False,
        *args,
        **kwargs,
    ) -> None:
        try:
            logger.info("Starting RSS source generation task")
            self.run_pipeline(
                "rsssource",
                load=load,
                download=download,
                merge=merge,
                dump=dump,
                sync=sync,
                upload=upload,
                publish=publish,
                *args,
                **kwargs,
            )
            logger.info("RSS source generation task completed successfully")
        except Exception as e:
            logger.error(f"RSS source generation failed: {e}")
            raise
