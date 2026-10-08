"""真实 HTTP 层：`engine.fetch.Fetcher` 协议的 requests 实现 + 编码判定。

`engine/` 不许 import requests（分层禁令，见 `engine/__init__.py`），所以这一层
单独放。调用方把 `RequestsFetcher` 注入 `BookSourceEngine`。
"""

from . import charset
from .fetcher import DEFAULT_TIMEOUT, MAX_BODY_BYTES, MAX_RETRY, RequestsFetcher

__all__ = [
    "DEFAULT_TIMEOUT",
    "MAX_BODY_BYTES",
    "MAX_RETRY",
    "RequestsFetcher",
    "charset",
]
