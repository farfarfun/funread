"""HTML report builder for source snapshots."""

from datetime import datetime
from typing import Any, Dict
import traceback

from dominate.tags import *
from nltlog import getLogger


logger = getLogger("funread")

ORG_REPOS = [
    ("funsecret", "farfarfun/funsecret"),
    ("fundrive", "farfarfun/fundrive"),
    ("funread", "farfarfun/funread"),
]

RSS_SOURCES = [
    {
        "label": "github",
        "href": "yuedu://rssSource/importonline?src=https://github.com/farfarfun/funread-cache/raw/master/funread/legado/rss/rss-main.json",
    },
    {
        "label": "gitee",
        "href": "yuedu://rssSource/importonline?src=https://gitee.com/farfarfun/funread-cache/raw/master/funread/legado/rss/rss-main.json",
    },
    {
        "label": "gitlink",
        "href": "yuedu://rssSource/importonline?src=https://gitlink.org.cn/farfarfun/funread-cache/raw/master/funread/legado/rss/rss-main.json",
    },
]


class BuildInfoSection:
    def generate_build_repo(self, category: str, repo: str) -> None:
        with li(category + ":"):
            a("github", href=f"https://github.com/{repo}")
            a("gitee", href=f"https://gitee.com/{repo}")
            a("gitlink", href=f"https://gitlink.org.cn/{repo}")

    def generate_org(self) -> None:
        build_type_div = div()
        build_type_fond = b()
        build_type_fond += font("farfarfun")
        build_type_div += build_type_fond
        with ul():
            for category, repo in ORG_REPOS:
                self.generate_build_repo(category, repo)
            with li("订阅源:"):
                for source in RSS_SOURCES:
                    a(source["label"], href=source["href"])

    def generate(self) -> None:
        br()
        div(b(font("Build Information", color="#0B610B")))
        div(hr(size=2, alignment="center", width="100%"))
        div((b(font("Cause: Started by upstream pipeline job "))))
        self.generate_org()
        img(
            src="https://profile-avatar.csdnimg.cn/2e9aaf5666c044d7aa273e086f9878d0_n1007530194.jpg"
        )
        br()
        p("** This is an automatically generated email by jenkins job. **")
        p("Feel free to connect 1007530194@qq.com if you have any question.")
        p(f"update time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


class SourceReportBuilder:
    """Build HTML report for uploaded source batches."""

    style_applied = """
        body { font-family: verdana,arial,sans-serif; font-size: 11px; }
        table.gridtable { color: #333333; border-width: 1px; border-color: #666666; border-collapse: collapse; font-size: 11px; }
        table.gridtable th { border-width: 1px; padding: 8px; border-style: solid; border-color: #666666; background-color: #DDEBF7; }
        table.gridtable td { border-width: 1px; padding: 8px; border-style: solid; border-color: #666666; background-color: #ffffff; text-align: center; }
        table.gridtable td.failed { color: #ED5F5F; }
        table.gridtable td.passrate { font-weight: bold; color: green; }
        li { margin-top: 5px; }
        div { margin-top: 10px; }
    """

    def __init__(self, context: Any):
        self.context = context

    @staticmethod
    def format_file_size(size: Any) -> str:
        try:
            value = float(size)
        except (TypeError, ValueError):
            return str(size)
        units = ["B", "KB", "MB", "GB", "TB"]
        unit_index = 0
        while value >= 1024 and unit_index < len(units) - 1:
            value /= 1024
            unit_index += 1
        if unit_index == 0:
            return f"{int(value)} {units[unit_index]}"
        return f"{value:.1f} {units[unit_index]}"

    def extract_source_count(self, file: Dict[str, Any]) -> str:
        fid = str(file.get("fid", ""))
        name = str(file.get("name", ""))
        if not name.endswith(".json"):
            return "-"
        if fid in self.context._source_count_cache:
            return self.context._source_count_cache[fid]
        if name in self.context._source_count_cache:
            return self.context._source_count_cache[name]
        count = file.get("source_count")
        if count is None:
            return "-"
        count_text = str(count)
        self.context._remember_source_count(fid, name, count=count_text)
        return count_text

    def set_table_head(self) -> None:
        with tr():
            th("文件")
            th("源个数")
            th("大小")
            th("github")
            th("gitee")

    def fill_table_data(self, file: Dict[str, Any]) -> None:
        data_tr = tr()
        data_tr += td(file["name"])
        data_tr += td(self.extract_source_count(file))
        data_tr += td(self.format_file_size(file.get("size", 0)))
        github_link = td()
        github_link += a(
            "github",
            href=f"yuedu://{self.context.source_type}/importonline?src=https://github.com/{self.context.repo_str}/raw/master/{file['fid']}",
        )
        data_tr += github_link
        gitee_link = td()
        gitee_link += a(
            "gitee",
            href=f"yuedu://{self.context.source_type}/importonline?src=https://gitee.com/{self.context.repo_str}/raw/master/{file['fid']}",
        )
        data_tr += gitee_link

    def generate_table(self) -> None:
        result_div = div(id="test case result")
        with result_div.add(table(cls="gridtable")).add(tbody()):
            self.set_table_head()
            try:
                files = self.context.drive.get_file_list(self.context.dir_path)
                for file in files:
                    if file["name"].endswith(".json"):
                        self.fill_table_data(file)
            except Exception as e:
                logger.error(f"Failed to generate table: {e}\n{traceback.format_exc()}")

    def generate_html_report(self) -> str:
        html_root = html()
        with html_root.add(head()):
            style(self.style_applied, type="text/css")
        with html_root.add(body()):
            hello_div = div(id="hello")
            hello_div.add(p("Hi All,"))
            hello_div.add(p("This is today's source report"))
            self.generate_table()
            BuildInfoSection().generate()
        return html_root.render()
