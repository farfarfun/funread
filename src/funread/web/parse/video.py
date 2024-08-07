from typing import List

from pydantic import BaseModel

from .base import BaseParse


class VideoInfo(BaseModel):
    text: str = ""
    pic_url: str = ""
    video_url: str = ""
    description: str = None


class VideoListInfo(BaseModel):
    page_no: int = 1
    page_size = 10
    video_list: List[VideoInfo] = []


class ParseVideo(BaseParse):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def parse_video_detail(self) -> VideoInfo:
        pass

    def parse_video_list(self) -> VideoListInfo:
        pass
