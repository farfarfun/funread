"""`{{}}` 里的安全算术求值。

`{{page}}`、`{{(page-1)*20}}`、`{{page*20-20}}` 这类不该为了几个加减号就去起 JS 引擎。
用 ast 白名单遍历：只放过字面量、`key`/`page` 两个名字和四则运算，
禁止 Call / Attribute / Subscript / 推导式等一切能逃逸的节点。
"""

import ast
from typing import Any, Dict, Optional

_ALLOWED_NODES = (
    ast.Expression,
    ast.BinOp,
    ast.UnaryOp,
    ast.Constant,
    ast.Name,
    ast.Load,
    ast.Add,
    ast.Sub,
    ast.Mult,
    ast.Div,
    ast.FloorDiv,
    ast.Mod,
    ast.Pow,
    ast.USub,
    ast.UAdd,
)

_ALLOWED_NAMES = {"key", "page"}


class MiniExpr:
    """只认纯算术的小表达式求值器。"""

    @staticmethod
    def try_eval(expression: str, variables: Dict[str, Any]) -> Optional[Any]:
        """能安全求值就返回结果，否则返回 None（交给 JS 运行时）。"""
        expression = expression.strip()
        if not expression:
            return None
        try:
            tree = ast.parse(expression, mode="eval")
        except SyntaxError:
            return None

        for node in ast.walk(tree):
            if not isinstance(node, _ALLOWED_NODES):
                return None
            if isinstance(node, ast.Name) and node.id not in _ALLOWED_NAMES:
                return None
            if isinstance(node, ast.Constant) and not isinstance(node.value, (int, float, str)):
                return None

        env = {name: variables.get(name) for name in _ALLOWED_NAMES}
        if any(isinstance(node, ast.Name) and env.get(node.id) is None for node in ast.walk(tree)):
            return None

        try:
            value = eval(  # noqa: S307 - 已按白名单校验过 AST，无可调用节点
                compile(tree, "<miniexpr>", "eval"), {"__builtins__": {}}, env
            )
        except Exception:
            return None
        if isinstance(value, float) and value.is_integer():
            return int(value)
        return value


__all__ = ["MiniExpr"]
