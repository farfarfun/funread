from datetime import datetime

import requests
from fundrive import GithubDrive
from funfake.headers import Headers
from funtask import Task

faker = Headers()


class UpdateRssTask(Task):
    def __init__(self):
        self.drive = GithubDrive()
        self.drive.login("farfarfun/funread-cache")
        super(UpdateRssTask, self).__init__()

    def random_icon(self):
        url = "https://api.thecatapi.com/v1/images/search?size=full"
        response = requests.get(url, headers=faker.generate()).json()
        return response[0]["url"]

    def update_book(self, dir_path="funread/legado/snapshot/lasted"):
        dl = []
        for dir_info in self.drive.get_dir_list(dir_path):
            dl.append(
                {
                    "title": dir_info["name"],
                    "pic": self.random_icon(),
                    "url": f"https://farfarfun.github.io/funread-cache/{dir_info['path']}/index.html",
                    "description": "this is content",
                }
            )

        dl.append(
            {
                "title": "源仓库(新)",
                "url": "https://link3.cc/yckceo",
            }
        )

        dl.append({"title": "开源阅读-语雀文档", "url": "https://www.yuque.com/legado"})
        dl.append({"title": "喵公子书源", "url": "http://yuedu.miaogongzi.net/gx.html"})
        dl.append({"title": "「阅读」APP 源-aoaostar", "url": "https://legado.aoaostar.com/"})
        dl.append({"title": "yiove", "url": "https://shuyuan.yiove.com/"})

        #
        for line in dl:
            if "pic" not in line:
                line["pic"] = self.random_icon()
            if "time" not in line:
                line["time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        data1 = {
            "name": "test",
            "next": "https://gitee.com/farfarfun/funread-cache/raw/master/funread/legado/snapshot/lasted/funread.json",
            "list": dl,
        }
        self.drive.upload_file(git_path=f"{dir_path}/source.json", content=data1)

    def update_main(self):
        rss = {
            "源": "https://gitee.com/farfarfun/funread-cache/raw/master/funread/legado/snapshot/lasted/source.json",
            "源(备)": "https://github.com/farfarfun/funread-cache/raw/master/funread/legado/snapshot/lasted/source.json",
        }

        data2 = [
            {
                "lastUpdateTime": int(datetime.now().timestamp()),
                "sourceName": "funread",
                "sourceIcon": self.random_icon(),
                "sourceUrl": "https://github.com/farfarfun",
                "loadWithBaseUrl": False,
                "singleUrl": False,
                "sortUrl": "\n".join([f"{k}::{v}" for k, v in rss.items()]),
                "ruleArticles": "$.list[*]",
                "ruleNextArticles": "$.next",
                "ruleTitle": "$.title",
                "rulePubDate": "⏰{{$.time}}",
                "ruleImage": "$.pic",
                "ruleLink": "$.url",
                "sourceGroup": "VIP",
                "customOrder": -9999999,
                "enabled": True,
            }
        ]
        self.drive.upload_file(git_path="funread/legado/snapshot/lasted/funread.json", content=data2)

    def run(self):
        self.update_book()
        self.update_main()


UpdateRssTask().run()
