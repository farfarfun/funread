from pydantic import BaseModel

from .base import BaseParse


class VideoInfo(BaseModel):
    text: str = ""
    pic_url: str = ""
    video_url: str = ""
    description: str | None = None


class VideoListInfo(BaseModel):
    page_no: int = 1
    page_size: int = 10
    video_list: list[VideoInfo] = []


class ParseVideo(BaseParse):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def parse_video_detail(self) -> VideoInfo:
        pass

    def parse_video_list(self) -> VideoListInfo:
        pass
