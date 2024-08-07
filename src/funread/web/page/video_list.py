from funread.web.parse.video import ParseVideo

from nicegui import ui
from nicegui.events import ValueChangeEventArguments
from .video import video_page, VideoInfo


def show(event: ValueChangeEventArguments):
    name = type(event.sender).__name__
    ui.notify(f"{name}: {event.value}")


def card(video: VideoInfo):
    with ui.card():
        ui.image(video.pic_url)
        with ui.row():
            ui.link(video.text, video_page(video))


def video_list_page(parse: ParseVideo, tab_id=0):
    @ui.page("/videos")
    def video_list_play(rows=10, cols=2):
        ui.button("Button", on_click=lambda: ui.notify("Click"))
        videos = parse.parse_video_list()

        index = 0
        for i in range(rows):
            with ui.row():
                for j in range(cols):
                    if index >= len(videos.video_list):
                        break
                    card(videos.video_list[index])

    return video_list_play


def video_list_play(parse: ParseVideo, tab_id=0, rows=10, cols=2):
    ui.button("Button", on_click=lambda: ui.notify("Click"))
    videos = parse.parse_video_list()
    print(videos)
    index = 0
    for i in range(rows):
        with ui.row():
            for j in range(cols):
                if index >= len(videos.video_list):
                    break
                card(videos.video_list[index])


def video_list_tabs_page(parse: ParseVideo, tab_id=0):
    @ui.page("/videosssss")
    def video_list_tabs_play(rows=10, cols=2):
        parse_tabs = parse.tabs()
        if len(parse_tabs) == 0:
            video_list_play(parse, tab_id=tab_id)
        else:
            with ui.tabs().classes("w-full") as tabs:
                for name in parse_tabs.keys():
                    tab = ui.tab(name)
                    with ui.tab_panels(tabs, value=tab).classes("w-full"):
                        with ui.tab_panel(tab):
                            video_list_play(parse, tab_id=tab_id)

    return video_list_tabs_play


ui.run()
