from funread.web.parse.video import VideoInfo

from nicegui import ui


def video_page(video_info: VideoInfo = None):
    @ui.page("/media/video/")
    def video_play():
        v = ui.video(src=video_info.video_url)
        v.on("ended", lambda _: ui.notify("Video playback completed"))

    return video_play
