"""规则引擎异常层级。

设计约束：选择器匹配不到返回空值而不抛异常；只有「我们实现不了」或「必填字段为空」
才抛。`UnsupportedFeatureError` 是上层用来给源打标记的那一族，必须一路冒泡到服务层，
绝不在引擎内部吞掉或降级成空结果。
"""

from typing import Optional


class RuleError(Exception):
    """所有规则相关异常的基类。"""

    def __init__(
        self,
        message: str,
        *,
        rule: Optional[str] = None,
        source_url: Optional[str] = None,
    ):
        super().__init__(message)
        self.message = message
        self.rule = rule
        self.source_url = source_url

    def __str__(self) -> str:
        parts = [self.message]
        if self.rule:
            snippet = self.rule if len(self.rule) <= 160 else self.rule[:157] + "..."
            parts.append(f"rule={snippet!r}")
        if self.source_url:
            parts.append(f"source={self.source_url}")
        return " | ".join(parts)


class RuleSyntaxError(RuleError):
    """规则串的语法我们解析不了。"""


class RuleEmptyError(RuleError):
    """必填字段求值为空。"""


class FetchError(RuleError):
    """抓取失败（网络错误、非 2xx、超时）。带上 url 与 status 便于定位。"""

    def __init__(
        self,
        message: str,
        *,
        url: Optional[str] = None,
        status: Optional[int] = None,
        rule: Optional[str] = None,
        source_url: Optional[str] = None,
    ):
        super().__init__(message, rule=rule, source_url=source_url)
        self.url = url
        self.status = status

    def __str__(self) -> str:
        parts = [super().__str__()]
        if self.url:
            parts.append(f"url={self.url}")
        if self.status is not None:
            parts.append(f"status={self.status}")
        return " | ".join(parts)


class UnsupportedFeatureError(RuleError):
    """源用到了当前阶段不支持的能力，可被上层标记后跳过。"""

    kind = "other"


class JsNotSupportedError(UnsupportedFeatureError):
    """规则需要 JS 求值，但当前注入的是 NullJsRuntime。"""

    kind = "js"


class WebViewNotSupportedError(UnsupportedFeatureError):
    """规则需要 WebView 渲染或请求拦截。"""

    kind = "webview"


class JsonPathNotSupportedError(RuleSyntaxError):
    """JSONPath 表达式用了 Jayway 专有语法，jsonpath-ng 不认。"""


__all__ = [
    "FetchError",
    "JsNotSupportedError",
    "JsonPathNotSupportedError",
    "RuleEmptyError",
    "RuleError",
    "RuleSyntaxError",
    "UnsupportedFeatureError",
    "WebViewNotSupportedError",
]
