"""phase 1 的 JS 运行时：显式失败。

**不降级、不返回空**。实测过「丢掉末尾 @js: 管道、保留选择器结果」这个折中：
只能救回 21% 的单点阻塞源，而 48% 的单点阻塞落在 URL 生成型字段上 —— 降级后拿到
裸 id、发出错误请求，比直接报错更糟。所以这里一律抛。
"""

from typing import Any

from ..errors import JsNotSupportedError
from .base import JsContext


class NullJsRuntime:
    name = "null"

    def available(self) -> bool:
        return False

    def eval(self, script: str, context: JsContext) -> Any:
        snippet = script.strip()
        if len(snippet) > 120:
            snippet = snippet[:117] + "..."
        raise JsNotSupportedError(
            f"该规则需要 JS 求值，当前阶段不支持：{snippet!r}（安装 funread[js] 后启用）",
            rule=script,
        )


__all__ = ["NullJsRuntime"]
