from funread.legado.task import (
    ReadODSSourceDataTask,
    ReadODSProgressDataTask,
    UpdateRssTask,
    GenerateSourceTask,
)
from funread.legado.task import ReadODSUrlDataTask


def run1():
    task = ReadODSUrlDataTask()
    task.snapshot_download()
    task.url_manage.delete_all()
    # task.add_book_source("https://bitbucket.org/xiu2/yuedu/raw/master/shuyuan")
    task.add_rss_source("https://jt12.de/SYV2/2023/03/17/0/02/42/167898256264133da2869d4.json")
    task.snapshot_upload()


def run2():
    task = ReadODSSourceDataTask()
    task.run()


def run3():
    task = ReadODSProgressDataTask()
    task.run()


def run4():
    task = GenerateSourceTask()
    task.run()
    task.update_rss()


def run5():
    task = UpdateRssTask()
    task.run()
    task.update_main()


run1()
