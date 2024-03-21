from funread.legado.task import ReadODSUrlDataTask
task=ReadODSUrlDataTask()
task.snapshot_download()
task.add_book_source("https://bitbucket.org/xiu2/yuedu/raw/master/shuyuan")
task.add_rss_source("https://www.yckceo1.com/yuedu/rsss/json/id/43.json")
print(len(task.url_manage.select_all()))
task.snapshot_upload()
