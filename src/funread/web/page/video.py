from collections.abc import Callable

from nicegui import ui

from funread.web.parse.video import VideoInfo


def video_page(video_info: VideoInfo | None = None) -> Callable[[], None]:
    """创建视频播放页面。

    Args:
        video_info: 要播放的视频信息；为空时页面不预设视频地址。

    Returns:
        已注册到 NiceGUI 的视频页面处理函数。
    """
    @ui.page("/media/video/")
    def video_play() -> None:
        v = ui.video(src=video_info.video_url if video_info else "")
        v.on("ended", lambda _: ui.notify("Video playback completed"))

    return video_play
