from datetime import datetime

from dominate.tags import *
from fundrive import GithubDrive
from funfile import funos
from funread.legado.manage.download.rss import RSSSourceDownload
from funsecret import read_secret
from funtask import Task

"""
dominate example: https://blog.csdn.net/wumingxiaoyao/article/details/122894671
"""


class GenerateCommon:
    def generate_build_repo(self, category, repo):
        with li(category + ":"):
            a("github", href=f"https://github.com/{repo}")
            a("gitee", href=f"https://gitee.com/{repo}")
            a("gitlink", href=f"https://gitlink.org.cn/{repo}")

    def generate_org(self):
        build_type_div = div()
        build_type_fond = b()
        build_type_fond += font("farfarfun")
        build_type_div += build_type_fond
        with ul():
            self.generate_build_repo("funsecret", "farfarfun/funsecret")
            self.generate_build_repo("fundrive", "farfarfun/fundrive")
            self.generate_build_repo("funread", "farfarfun/funread")
            with li("订阅源:"):
                a("github",
                  href="yuedu://rssSource/importonline?src=https://github.com/farfarfun/funread-cache/raw/master/funread/legado/rss/rss-main.json")
                a("gitee",
                  href="yuedu://rssSource/importonline?src=https://gitee.com/farfarfun/funread-cache/raw/master/funread/legado/rss/rss-main.json")
                a("gitlink",
                  href="yuedu://rssSource/importonline?src=https://gitlink.org.cn/farfarfun/funread-cache/raw/master/funread/legado/rss/rss-main.json")

    def generate(self):
        br()
        div(b(font("Build Information", color="#0B610B")))
        div(hr(size=2, alignment="center", width="100%"))
        div((b(font("Cause: Started by upstream pipeline job "))))
        self.generate_org()
        img(src="https://profile-avatar.csdnimg.cn/2e9aaf5666c044d7aa273e086f9878d0_n1007530194.jpg")
        br()
        p("** This is an automatically generated email by jenkins job. **")
        p("Feel free to connect 1007530194@qq.com if you have any question.")
        p(f'update time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')


class GenerateSourceType:
    style_applied = """
            body{
    			font-family: verdana,arial,sans-serif;
    			font-size:11px;
    		}
    		table.gridtable {
    			color: #333333;
    			border-width: 1px;
    			border-color: #666666;
    			border-collapse: collapse;
                font-size:11px;
    		}
    		table.gridtable th {
    			border-width: 1px;
    			padding: 8px;
    			border-style: solid;
    			border-color: #666666;
    			background-color: #DDEBF7;
    		}
    		table.gridtable td {
    			border-width: 1px;
    			padding: 8px;
    			border-style: solid;
    			border-color: #666666;
    			background-color: #ffffff;
    			text-align:center;
    		}
    		table.gridtable td.failed {
    			color:#ED5F5F;
    		}
    		table.gridtable td.passrate {
    			font-weight:bold;
    			color:green;
    		}
    		li {
    			margin-top:5px;
    		}
            div{
                margin-top:10px;
            }
        """

    def __init__(self, dir_path="funread/legado/book/snapshot/20231011", source_type="booksource"):
        self.repo_str = "farfarfun/funread-cache"
        self.dir_path = dir_path
        self.source_type = source_type
        self.drive = GithubDrive()
        self.drive.login(repo_str=self.repo_str)

    def set_table_head(self):
        with tr():
            th("文件")
            th("大小")
            th("github")
            th("gitee")

    def fill_table_data(self, file):
        data_tr = tr()
        data_tr += td(file["name"])
        data_tr += td(file["size"])
        link_td = td()
        link_td += a(
            "github",
            href=f"yuedu://{self.source_type}/importonline?src=https://github.com/{self.repo_str}/raw/master/{file['path']}",
        )
        data_tr += link_td
        link_td = td()
        link_td += a(
            "gitee",
            href=f"yuedu://{self.source_type}/importonline?src=https://gitee.com/{self.repo_str}/raw/master/{file['path']}",
        )
        data_tr += link_td

    def generate_table(self):
        result_div = div(id="test case result")
        with result_div.add(table(cls="gridtable")).add(tbody()):
            self.set_table_head()

            files = self.drive.get_file_list(self.dir_path)
            for file in files:
                if file["name"].endswith(".json"):
                    self.fill_table_data(file)

    def generate_html_report(self):
        html_root = html()
        # html head
        with html_root.add(head()):
            style(self.style_applied, type="text/css")

        # html body
        with html_root.add(body()):
            hello_div = div(id="hello")
            hello_div.add(p("Hi All,"))
            hello_div.add(p("This is today's " " API Test Report in "))
            self.generate_table()
            GenerateCommon().generate()

        return html_root.render()

    def split_and_upload(self):
        path = read_secret(cate1='funread', cate2='cache', cate3='path', cate4='root')
        with RSSSourceDownload(path=path, cate1="rss") as runner:
            funos.delete(runner.path_pkl)
            funos.delete(runner.path_bok)
            runner.loads_zip()
            i = 1000
            for data in runner.export_sources(size=3000):
                if len(data) > 0:
                    self.drive.upload_file(content=data, git_path=f"{self.dir_path}/progress-{i}.json")
                    i += 1

    def update_rss(self):
        print("update source rss")
        self.drive.upload_file(git_path=f"{self.dir_path}/index.html", content=self.generate_html_report())
        print(len(self.generate_html_report()))


class GenerateSourceTask(Task):
    def __init__(self, dir_path=f"funread/legado/snapshot/lasted", source_type="booksource", *args, **kwargs):
        self.repo_str = "farfarfun/funread-cache"
        self.dir_path = dir_path
        self.source_type = source_type
        self.drive = GithubDrive()
        self.drive.login(repo_str=self.repo_str)
        super(GenerateSourceTask, self).__init__(*args, **kwargs)

    def run_book(self, *args, **kwargs):
        generate_source = GenerateSourceType(source_type="booksource", dir_path=f"{self.dir_path}/book")
        generate_source.split_and_upload()
        generate_source.update_rss()

    def run_rss(self, *args, **kwargs):
        generate_source = GenerateSourceType(source_type="rsssource", dir_path=f"{self.dir_path}/rss")
        generate_source.split_and_upload()
        generate_source.update_rss()
