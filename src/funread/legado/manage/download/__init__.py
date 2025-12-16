"""源下载和管理模块"""

from .base import DownloadSource
from .book import BookSourceDownload, BookSourceFormat
from .generate import GenerateSourceTask
from .rss import RSSSourceDownload, RSSSourceFormat

__all__ = [
    "DownloadSource",
    "BookSourceFormat",
    "BookSourceDownload",
    "GenerateSourceTask",
    "RSSSourceDownload",
    "RSSSourceFormat",
]
