"""工具函数模块"""

import re
from typing import Optional
from urllib.parse import urlparse


def url_to_hostname(url: str) -> Optional[str]:
    """
    从 URL 中提取主机名

    Args:
        url: 完整的 URL 字符串

    Returns:
        主机名字符串，如果解析失败则返回 None
    """
    try:
        parsed = urlparse(url)
        return parsed.hostname
    except Exception:
        return None


def retain_zh_ch_dig(text: str) -> str:
    """
    保留中文字符、英文字母、数字和方括号

    Args:
        text: 输入文本

    Returns:
        清理后的文本，只包含中文字符、英文字母、数字和方括号
    """
    return re.sub(r"[^\u4e00-\u9fa5a-zA-Z0-9\[\]]+", "", text)
