"""对单个源 JSON 跑通「搜索 → 详情 → 目录 → 正文」的排查工具。

    python -m funread.legado.net.probe <source.json> <关键词> [--chapter N]

源 JSON 可以是裸书源，也可以是采集流程产出的带包装对象（`final`/`merged`/
`candidate`）—— `SourceSpec` 会自己剥。会真打网，只用来人工确认一个源能不能读。
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict

from ..engine import BookSourceEngine, load_source
from ..engine.errors import RuleError, UnsupportedFeatureError
from .fetcher import RequestsFetcher


def _load(path: Path) -> Dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        if not data:
            raise SystemExit(f"{path} 是空数组")
        data = data[0]
    if not isinstance(data, dict):
        raise SystemExit(f"{path} 不是一个源对象")
    return data


def _line(char: str = "-", width: int = 60) -> None:
    print(char * width)


def run(path: Path, keyword: str, chapter_index: int, limit: int) -> int:
    spec = load_source(_load(path))

    print(f"源名：{spec.name}")
    print(f"源址：{spec.url}")
    missing = spec.missing_core_fields()
    if missing:
        print(f"⚠️  核心字段缺失：{', '.join(missing)}")
    if spec.needs_js():
        print("⚠️  该源含 JS 规则，当前阶段跑不通（phase 2 接 quickjs）")
    _line("=")

    with RequestsFetcher() as fetcher:
        engine = BookSourceEngine(spec, fetcher)

        books = engine.search(keyword)
        print(f"搜索「{keyword}」→ {len(books)} 条")
        for book in books[:limit]:
            print(f"  · {book.name} / {book.author or '佚名'} → {book.book_url}")
        if not books:
            print("搜索没有结果，后面的阶段跳过")
            return 1
        _line()

        info = engine.book_info(books[0])
        print(f"详情：{info.name} / {info.author or '佚名'}")
        print(f"  分类：{info.kind or '-'}  字数：{info.word_count or '-'}")
        print(f"  简介：{(info.intro or '-')[:120]}")
        print(f"  封面：{info.cover_url or '-'}")
        print(f"  目录：{info.toc_url}")
        if info.variables:
            print(f"  变量：{info.variables}")
        _line()

        chapters = engine.toc(info)
        print(f"目录 → {len(chapters)} 章")
        for chapter in chapters[:limit]:
            print(f"  {chapter.index:>4}. {chapter.name} → {chapter.url}")
        if not chapters:
            print("目录为空，正文阶段跳过")
            return 1
        _line()

        if not 0 <= chapter_index < len(chapters):
            print(f"--chapter {chapter_index} 超出范围（0..{len(chapters) - 1}）")
            return 1
        target = chapters[chapter_index]
        content = engine.content(target, info=info)
        print(f"正文：{content.title}（{len(content.pages)} 页，{len(content.text)} 字）")
        _line()
        print(content.text[:800])
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="funread-probe", description=__doc__)
    parser.add_argument("source", type=Path, help="书源 JSON 文件")
    parser.add_argument("keyword", help="搜索关键词")
    parser.add_argument("--chapter", type=int, default=0, help="取第几章正文（默认 0）")
    parser.add_argument("--limit", type=int, default=5, help="列表最多打印几条")
    args = parser.parse_args(argv)

    try:
        return run(args.source, args.keyword, args.chapter, args.limit)
    except UnsupportedFeatureError as exc:
        #  JS / WebView 这类「暂不支持」，和规则写错区分开报
        print(f"✗ 该源暂不支持（{exc.kind}）：{exc}", file=sys.stderr)
        return 2
    except RuleError as exc:
        print(f"✗ 规则执行失败：{exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
