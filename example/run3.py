import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from funread.legado.manage.download import GenerateSourceTask
from funread.legado.manage.publish import UpdateRssTask
from funread.legado.manage.source import MergeSourceTask


def run_generate():
    task = GenerateSourceTask()
    task.run_book()
    task.run_rss()


def run_merge():
    task = MergeSourceTask()
    task.run_book()
    task.run_rss()


def run_publish():
    task = UpdateRssTask()
    task.run()


if __name__ == "__main__":
    run_generate()
