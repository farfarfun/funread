"""批量实测一批书源的搜索链路，产出可复现、可对比的统计。

    python -m funread.legado.net.survey --count 40 --needs-js --out /tmp/survey.jsonl

`probe.py` 是单源排查用的；这个是给「接上 quickjs 之后到底多了多少能用的源」
这类问题准备的 —— 2026-10-16 那次实测的脚本没有提交，哪 40 个源也没记录，
结果修完两个 bug 之后想重测都没法对比。所以这里的要求是：

- **抽样确定**：按 url_id 升序取前 N 个满足条件的源，`--offset` 换一批。
  同一份归档、同样的参数，两次跑的是同一批源。
- **结果落盘**：JSONL，首行是 git SHA、日期、参数，之后每源一行。
- **分类口径固定**：和 `012-known-tech-debt.md` 那张表一致，这样新旧两次能逐格比。

会真打网。不碰数据库 —— 结果只进文件，不进 `reader_source_prefs`。
"""

import argparse
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

from ..engine import (
    BookSourceEngine,
    FetchError,
    JsNotSupportedError,
    RuleError,
    SourceSpec,
    WebViewNotSupportedError,
    load_source,
    scan_features,
)
from .fetcher import RequestsFetcher

#: 分类名。顺序就是汇总表的顺序。
CATEGORIES = (
    "net:unreachable",  # 连不上：DNS / 拒绝连接 / 超时
    "net:http",  # 连上了但非 2xx
    "net:other",  # 其它网络层错误（TLS、编码、重定向环…）
    "js",  # JS 执行失败（缺宿主方法、脚本报错）
    "webview",  # 规则要浏览器环境
    "rule",  # 非 JS 的规则求值错误
    "empty",  # 跑通但没解析出书
    "ok",  # 搜到书
    "other",  # 没归进上面的异常
)

LABELS = {
    "net:unreachable": "网络：站点连不上",
    "net:http": "网络：HTTP 错误码",
    "net:other": "网络：其它",
    "js": "JS 执行失败",
    "webview": "WebView",
    "rule": "规则求值失败",
    "empty": "跑通但没解析出书",
    "ok": "成功",
    "other": "其它异常",
}


def classify(outcome: Any) -> Tuple[str, str]:
    """把一次搜索的结果（列表或异常）归到一个分类，附一句细节。"""
    if isinstance(outcome, list):
        return ("ok" if outcome else "empty"), f"{len(outcome)} 条"
    exc = outcome
    detail = f"{type(exc).__name__}: {str(exc)[:160]}"
    if isinstance(exc, WebViewNotSupportedError):
        return "webview", detail
    if isinstance(exc, JsNotSupportedError):
        return "js", detail
    if isinstance(exc, FetchError):
        status = getattr(exc, "status", None)
        return ("net:http" if status else "net:unreachable"), detail
    if isinstance(exc, RuleError):
        #  宿主调用失败会被 _guard 记进消息里，看到它就是 JS 这一侧的问题
        text = str(exc).lower()
        if "js" in text or "quickjs" in text or "宿主" in text or "java." in text:
            return "js", detail
        return "rule", detail
    name = type(exc).__name__.lower()
    if any(key in name for key in ("connection", "timeout", "ssl", "dns", "socket", "http")):
        return "net:other", detail
    return "other", detail


def search_needs_js(spec: SourceSpec) -> bool:
    """搜索链路（searchUrl + ruleSearch 组）里有没有 JS。和 `needs_js()` 不同，
    后者扫全部规则 —— 目录、正文带 JS 的源这里不算，因为只跑搜索。"""
    rules = [spec.search_url, *spec.group_rules("ruleSearch").values()]
    return scan_features(rules)["needs_js"]


def iter_specs(hubs: str, source_type: str = "book") -> Iterator[Tuple[int, SourceSpec]]:
    """按 url_id 升序遍历归档。坏文件、解析不了的跳过。"""
    root = Path(hubs) / source_type / "source"
    if not root.is_dir():
        raise SystemExit(f"归档目录不存在：{root}")
    buckets = sorted(
        (p for p in root.iterdir() if p.is_dir() and p.name.split("-")[0].isdigit()),
        key=lambda p: int(p.name.split("-")[0]),
    )
    for bucket in buckets:
        files = sorted(
            (p for p in bucket.glob("*.json") if p.stem.isdigit()), key=lambda p: int(p.stem)
        )
        for path in files:
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                spec = load_source(data, source_type=source_type)
            except Exception:  # noqa: BLE001 — 坏文件就是坏文件，统计时不算
                continue
            yield int(path.stem), spec


def select(
    hubs: str, count: int, offset: int = 0, needs_js: bool = True
) -> List[Tuple[int, SourceSpec]]:
    """挑样本：规则完整，且（可选）搜索链路带 JS。确定性的 —— 同参数同结果。"""
    picked: List[Tuple[int, SourceSpec]] = []
    skipped = 0
    for url_id, spec in iter_specs(hubs):
        if not spec.is_complete:
            continue
        if needs_js and not search_needs_js(spec):
            continue
        if skipped < offset:
            skipped += 1
            continue
        picked.append((url_id, spec))
        if len(picked) >= count:
            break
    return picked


def run_one(spec: SourceSpec, keyword: str, fetcher_factory: Callable[[], Any]) -> Dict[str, Any]:
    started = time.monotonic()
    try:
        with fetcher_factory() as fetcher:
            outcome: Any = BookSourceEngine(spec, fetcher).search(keyword)
    except Exception as exc:  # noqa: BLE001 — 这里就是要把所有失败归类
        outcome = exc
    category, detail = classify(outcome)
    return {
        "name": spec.name,
        "url": spec.url,
        "category": category,
        "detail": detail,
        "elapsed": round(time.monotonic() - started, 2),
    }


def run_survey(
    samples: List[Tuple[int, SourceSpec]],
    keyword: str,
    fetcher_factory: Callable[[], Any],
    workers: int = 4,
    on_result: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {
            pool.submit(run_one, spec, keyword, fetcher_factory): url_id for url_id, spec in samples
        }
        for future in as_completed(futures):
            row = {"url_id": futures[future], **future.result()}
            results.append(row)
            if on_result:
                on_result(row)
    results.sort(key=lambda r: r["url_id"])
    return results


def summarize(results: List[Dict[str, Any]]) -> str:
    counts = {key: 0 for key in CATEGORIES}
    for row in results:
        counts[row["category"]] = counts.get(row["category"], 0) + 1
    total = len(results) or 1
    width = max(len(LABELS[k]) for k in CATEGORIES)
    lines = [f"{LABELS[k]:<{width}} {counts[k]:>4}" for k in CATEGORIES if counts[k]]
    net = sum(counts[k] for k in CATEGORIES if k.startswith("net:"))
    reachable = len(results) - net
    ok = counts["ok"]
    lines.append("")
    lines.append(
        f"样本 {len(results)}；源站挂了 {net}（{net * 100 // total}%）；"
        f"能连上 {reachable}，其中成功 {ok}"
        f"（{ok * 100 // reachable if reachable else 0}%）"
    )
    return "\n".join(lines)


def _git_sha() -> str:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
            cwd=os.path.dirname(os.path.abspath(__file__)),
        )
        return out.stdout.strip() or "unknown"
    except Exception:  # noqa: BLE001
        return "unknown"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="funread-survey", description=__doc__)
    parser.add_argument("--hubs", help="归档根目录（含 book/source）。不填按配置解析")
    parser.add_argument("--count", type=int, default=40)
    parser.add_argument("--offset", type=int, default=0, help="跳过前几个满足条件的源，换一批")
    parser.add_argument("--keyword", default="我的")
    parser.add_argument("--needs-js", action="store_true", help="只挑搜索链路带 JS 的源")
    parser.add_argument("--timeout", type=float, default=8.0, help="连接/读取各几秒")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--out", type=Path, help="JSONL 输出路径")
    args = parser.parse_args(argv)

    hubs = args.hubs
    if not hubs:
        from funread.base.config import resolve_cache_root

        hubs = resolve_cache_root()

    samples = select(hubs, args.count, offset=args.offset, needs_js=args.needs_js)
    print(f"选中 {len(samples)} 个源（offset={args.offset}, needs_js={args.needs_js}）", flush=True)

    header = {
        "git": _git_sha(),
        "date": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "args": {k: (str(v) if isinstance(v, Path) else v) for k, v in vars(args).items()},
        "url_ids": [url_id for url_id, _ in samples],
    }
    out = args.out.open("w", encoding="utf-8") if args.out else None
    if out:
        out.write(json.dumps(header, ensure_ascii=False) + "\n")

    def on_result(row: Dict[str, Any]) -> None:
        print(
            f"  {row['url_id']}  {LABELS[row['category']]:<10} {row['name'][:20]:<20} "
            f"{row['elapsed']:>6}s  {row['detail'][:70]}",
            flush=True,
        )
        if out:
            out.write(json.dumps(row, ensure_ascii=False) + "\n")
            out.flush()

    timeout = (args.timeout, args.timeout)
    results = run_survey(
        samples,
        args.keyword,
        lambda: RequestsFetcher(timeout=timeout),
        workers=args.workers,
        on_result=on_result,
    )
    if out:
        out.close()
    print()
    print(summarize(results))
    return 0


if __name__ == "__main__":
    sys.exit(main())
