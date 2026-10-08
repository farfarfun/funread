"""Legado 规则求值引擎（纯求值，不碰 IO）。

分层禁令：这个包只许依赖 lxml / cssselect / jsonpath-ng + stdlib。
**禁止 import requests / sqlalchemy / funsecret / funread.legado.manage** ——
一条 import 链就会把 funsecret 拉进来、连上生产 MySQL。
`tests/engine/test_no_heavy_imports.py` 会强制这条边界。
"""

from .book import MAX_CONTENT_PAGES, MAX_TOC_PAGES, BookSourceEngine, pick_best
from .errors import (
    FetchError,
    JsNotSupportedError,
    JsonPathNotSupportedError,
    RuleEmptyError,
    RuleError,
    RuleSyntaxError,
    UnsupportedFeatureError,
    WebViewNotSupportedError,
)
from .evaluator import RuleEvaluator, join_url, source_base_url
from .expr import MiniExpr
from .fetch import Fetcher, Page, Request, StaticFetcher, parse_url_options
from .js import JsContext, JsRuntime, NullJsRuntime, default_js_runtime
from .lexer import (
    parse_named_urls,
    parse_replace_regex,
    parse_rule,
    scan_features,
)
from .models import BookInfo, Chapter, ChapterContent, ExploreKind, RssArticle, SearchBook
from .source import CORE_FIELDS, SourceSpec, load_source
from .values import RuleValue
from .variables import VariableScope

__all__ = [
    "CORE_FIELDS",
    "MAX_CONTENT_PAGES",
    "MAX_TOC_PAGES",
    "BookInfo",
    "BookSourceEngine",
    "Chapter",
    "ChapterContent",
    "ExploreKind",
    "FetchError",
    "Fetcher",
    "JsContext",
    "JsNotSupportedError",
    "JsRuntime",
    "JsonPathNotSupportedError",
    "MiniExpr",
    "NullJsRuntime",
    "Page",
    "Request",
    "RssArticle",
    "RuleEmptyError",
    "RuleError",
    "RuleEvaluator",
    "RuleSyntaxError",
    "RuleValue",
    "SearchBook",
    "SourceSpec",
    "StaticFetcher",
    "UnsupportedFeatureError",
    "VariableScope",
    "WebViewNotSupportedError",
    "default_js_runtime",
    "join_url",
    "load_source",
    "parse_named_urls",
    "parse_replace_regex",
    "parse_rule",
    "parse_url_options",
    "pick_best",
    "scan_features",
    "source_base_url",
]
