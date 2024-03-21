# import argparse

# from funread.legado.task import GenerateSourceTask
# from funread.legado.task import ReadODSProgressDataTask
# from funread.legado.task import ReadODSSourceDataTask
# from funread.legado.task import ReadODSUrlDataTask
# from funread.legado.task import UpdateRssTask


# def funbuild():
#     parser = argparse.ArgumentParser(prog="PROG")
#     subparsers = parser.add_subparsers(help="sub-command help")

#     # 添加子命令
#     build_parser = subparsers.add_parser("url", help="format the source url")
#     build_parser.set_defaults(func=ReadODSUrlDataTask().run)

#     # 添加子命令
#     clean_history_parser = subparsers.add_parser("source", help="load the source")
#     clean_history_parser.set_defaults(func=ReadODSSourceDataTask().run)

#     # 添加子命令
#     pull_parser = subparsers.add_parser("progress", help="progress the source data.")
#     pull_parser.add_argument("--quiet", default=True, help="quiet")
#     pull_parser.set_defaults(func=ReadODSProgressDataTask().run)

#     # 添加子命令
#     push_parser = subparsers.add_parser("generate", help="git push")
#     push_parser.add_argument("--quiet", default=True, help="quiet")
#     push_parser.set_defaults(func=GenerateSourceTask().run)

#     # 添加子命令
#     tag_parser = subparsers.add_parser("update", help="git build tag")
#     tag_parser.set_defaults(func=UpdateRssTask().run)

#     args = parser.parse_args()
#     args.func(args)
