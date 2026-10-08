"""分层禁令：`engine/` 只许依赖 lxml / cssselect / jsonpath-ng + stdlib。

为什么要用子进程断言：`funsecret` 一被 import 就可能去读本机配置，而本机
`funread/cache/source/db_url` 指向**生产 MySQL**。只要 `engine` 的某条 import 链
不小心碰到 `funread.legado.manage`，就会把 sqlalchemy + funsecret 一起拉进来。
这条测试是那类事故的唯一结构性防线，别因为「跑着没报错」就删掉。
"""

import subprocess
import sys
import textwrap

FORBIDDEN = ("requests", "sqlalchemy", "funsecret", "funread.legado.manage")


def _run(code: str) -> str:
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, f"子进程失败：\n{result.stdout}\n{result.stderr}"
    return result.stdout.strip()


def test_engine_import_pulls_nothing_heavy():
    out = _run(
        f"""
        import sys
        import funread.legado.engine  # noqa: F401
        leaked = [m for m in {FORBIDDEN!r} if m in sys.modules]
        print(",".join(leaked))
        """
    )
    assert out == "", f"engine 导入时泄漏了重依赖：{out}"


def test_engine_full_evaluation_pulls_nothing_heavy():
    """不只是 import —— 跑完一轮真实求值（各方言都走到）也不许拉进来。"""
    out = _run(
        f"""
        import sys
        from funread.legado.engine import RuleEvaluator

        html = RuleEvaluator(
            '<html><body><ul id=l><li><a href="/b/1">书</a></li></ul></body></html>',
            base_url="https://example.com/",
        )
        assert html.strings("id.l@tag.a@text") == ["书"]
        assert html.urls("@css:#l a@href") == ["https://example.com/b/1"]
        assert html.strings("//li/a/text()") == ["书"]
        for row in html.rows("id.l@tag.li"):
            row.string("@put:{{k:tag.a@href}}")
        html.string("id.l@text##书##本")
        html.template("/s?q={{{{key}}}}")

        js = RuleEvaluator('{{"books":[{{"name":"甲"}}]}}')
        assert [r.string("$.name") for r in js.rows("$.books[*]")] == ["甲"]

        rows = RuleEvaluator('<li><a href="/c/1">章</a></li>').rows(
            r':<a href="(.*?)">(.*?)</a>'
        )
        assert [r.string("$2") for r in rows] == ["章"]

        leaked = [m for m in {FORBIDDEN!r} if m in sys.modules]
        print(",".join(leaked))
        """
    )
    assert out == "", f"engine 求值时泄漏了重依赖：{out}"


def test_engine_source_files_have_no_forbidden_imports():
    """静态兜底：函数体内的懒加载 import 不会被上面两条覆盖，这里用 AST 扫一遍。

    必须走 AST 而不是字符串匹配 —— `engine/__init__.py` 的 docstring 里正好写着
    「禁止 import requests」这句禁令，子串匹配会把禁令本身当成违规。
    """
    import ast
    import pathlib

    import funread.legado.engine as engine_pkg

    forbidden_tops = {name.split(".")[0] for name in FORBIDDEN}
    root = pathlib.Path(engine_pkg.__file__).parent
    offenders = []
    for path in sorted(root.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module or ""]
            else:
                continue
            for name in names:
                if name.split(".")[0] in forbidden_tops:
                    offenders.append(f"{path.name}:{node.lineno} → {name}")
    assert not offenders, "engine 里出现了禁用依赖：" + "; ".join(offenders)
