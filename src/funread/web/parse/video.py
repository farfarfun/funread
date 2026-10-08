from pydantic import BaseModel

from .base import BaseParse


class VideoInfo(BaseModel):
    """视频展示信息。

    Attributes:
        text: 视频标题。
        pic_url: 视频封面地址。
        video_url: 视频播放地址。
        description: 可选的视频说明。
    """
    text: str = ""
    pic_url: str = ""
    video_url: str = ""
    description: str | None = None


class VideoListInfo(BaseModel):
    """分页视频列表数据。

    Attributes:
        page_no: 当前页码。
        page_size: 每页数量。
        video_list: 当前页的视频信息。
    """
    page_no: int = 1
    page_size: int = 10
    video_list: list[VideoInfo] = []


class ParseVideo(BaseParse):
    """视频站点解析器的抽象基类。

    子类实现视频详情和列表解析接口，为网页页面提供数据。
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def parse_video_detail(self) -> VideoInfo:
        """解析视频详情，具体实现由站点解析器提供。"""
        raise NotImplementedError("子类必须实现 parse_video_detail")

    def parse_video_list(self) -> VideoListInfo:
        """解析视频列表，具体实现由站点解析器提供。"""
        raise NotImplementedError("子类必须实现 parse_video_list")
