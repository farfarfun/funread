"""Publish source HTML report task."""

from nltlog import getLogger

from ..reporting.builder import SourceReportBuilder
from ..reporting.remote import SourceRemoteManager
from .base import SourcePipelineTask


logger = getLogger("funread")


class PublishSourceReportTask(SourcePipelineTask):
    """Generate and publish the HTML report for a source directory."""

    def __init__(
        self,
        report_builder: SourceReportBuilder = None,
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
