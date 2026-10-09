"""入口页发布模块。"""

import json
import os
from datetime import datetime

import requests
from funfake.headers import Headers

try:
    from fundrive.drives.github import GithubDrive
except ImportError:  # pragma: no cover - fundrive 仅要求 Python >= 3.12
    GithubDrive = None


faker = Headers()


class UpdateEntrance:
    """生成并发布 Legado 入口页数据。

    依赖可选包 ``fundrive``（要求 Python >= 3.12）。
    """

    def __init__(self) -> None:
        """初始化 GitHub 存储驱动并登录目标仓库。

        Raises:
            ImportError: 未安装可选依赖 fundrive 时抛出，提示安装
                ``funread[publish]``（且需要 Python >= 3.12）。
        """
        if GithubDrive is None:
            raise ImportError(
                "UpdateEntrance 需要可选依赖 fundrive（Python >= 3.12）："
                "请执行 `pip install funread[publish]`"
            )
        self.drive = GithubDrive()
        self.drive.login(
            repo_owner="farfarfun",
            repo_name="funread-cache",
            branch="master",
        )

    def random_icon(self) -> str:
        """获取随机图标 URL。

        Returns:
            随机图片的 URL。
        """
        url = "https://api.thecatapi.com/v1/images/search?size=full"
        response = requests.get(url, headers=faker.generate()).json()
        return response[0]["url"]

    def update_book(self, dir_path: str = "funread/legado/snapshot/lasted") -> None:
        """更新书源入口文件。

        Args:
            dir_path: 远端快照目录。
        """
        dl = []
        for dir_info in self.drive.get_dir_list(dir_path):
            dl.append(
                {
                    "title": dir_info["name"],
                    "pic": self.random_icon(),
                    "url": f"https://farfarfun.github.io/funread-cache/{dir_info['fid']}/index.html",
                    "description": "this is content",
                }
            )

        dl.append({"title": "源仓库(新)", "url": "https://link3.cc/yckceo"})
        dl.append({"title": "开源阅读-语雀文档", "url": "https://www.yuque.com/legado"})
        dl.append({"title": "喵公子书源", "url": "http://yuedu.miaogongzi.net/gx.html"})
        dl.append({"title": "「阅读」APP 源-aoaostar", "url": "https://legado.aoaostar.com/"})
        dl.append({"title": "yiove", "url": "https://shuyuan.yiove.com/"})

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
        self.drive.upload_file(
            content=json.dumps(data1),
            fid=os.path.dirname(dir_info["fid"]),
            filepath=None,
            filename="source.json",
        )

    def update_main(self) -> None:
        """更新聚合入口文件。"""
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
        self.drive.upload_file(
            content=json.dumps(data2),
            fid="funread/legado/snapshot/lasted",
            filepath=None,
            filename="funread.json",
        )

    def run(self) -> None:
        """依次更新书源入口和聚合入口。"""
        self.update_book()
        self.update_main()
