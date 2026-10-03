from nicegui import ui

from funread.web.parse.video import VideoInfo


def video_page(video_info: VideoInfo = None):
    @ui.page("/media/video/")
    def video_play():
        v = ui.video(src=video_info.video_url)
        v.on("ended", lambda _: ui.notify("Video playback completed"))

    return video_play
