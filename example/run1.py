from funread.legado.manage.source import add_source_list_url


def main():
    add_source_list_url(
        url="https://bitbucket.org/xiu2/yuedu/raw/master/shuyuan",
        source_type="book",
    )
    add_source_list_url(
        url="https://www.yckceo1.com/yuedu/rsss/json/id/43.json",
        source_type="rss",
    )


if __name__ == "__main__":
    main()
