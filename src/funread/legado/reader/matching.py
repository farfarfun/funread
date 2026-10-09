"""换源时把「正在读的那一章」映射到新源的目录里。

为什么需要这一层：不同源的章节切分方式不一样。甲源的「第 500 章」和乙源的
「第 500 章」很可能不是同一段内容 —— 有的源把楔子算第一章，有的源把一章拆两页，
有的源多几条公告。所以换源之后：

- **序号不能直接沿用。** 沿用的话书架上写着「读到第 500 章」，但那个序号在新源下
  指向别的内容，用户点「续读」会跳到一个陌生位置，而且看不出哪里错了。
- **也不能直接归零。** 让一个读到第 500 章的人从头开始，比不换源更糟。

所以按章节**名字**找，名字找不到才按位置估，并且**明确告诉调用方是哪种**——
估出来的位置必然不准，界面必须能据此提示用户确认。

纯函数，不碰网络也不碰库：两份目录由调用方取好传进来。
"""

import re
from dataclasses import dataclass
from typing import List, Sequence

#: 章节名前缀的编号部分。`第100章 惊蛰` / `100.惊蛰` / `（100）惊蛰` 都剥掉，
#: 剩下的「惊蛰」才是跨源可比的那部分。
#:
#: 中文数字（`第一百章`）也认 —— 归档里相当一部分源用它，而同一本书在另一个源下
#: 很可能写成阿拉伯数字，不剥就永远匹配不上。
_CHAPTER_PREFIX = re.compile(
    r"^\s*[（(\[【]?\s*"
    r"(?:第\s*)?"
    r"(?:[0-9]+|[〇零一二三四五六七八九十百千万两]+)"
    r"\s*(?:[章节回话篇卷集]|[.、,，:：\-－—])?"
    r"\s*[）)\]】]?\s*"
)

#: 归一化时一并去掉的字符：空白与常见标点。源之间的标点习惯差异极大
#: （`惊蛰·上` / `惊蛰(上)` / `惊蛰 上`），不去掉就会把同一章判成不同章。
_NOISE = re.compile(r"[\s·,，.。、:：;；!！?？\"'“”‘’()（）\[\]【】{}<>《》~～\-－—_]+")


def normalize_chapter_name(name: str) -> str:
    """剥掉编号前缀与标点，留下可跨源比较的名字主体。

    归一化后为空（例如整章名就叫「第100章」，没有标题）时返回空串 —— 调用方必须
    把空串当作「不可比」，否则所有无标题章节会互相匹配上。
    """
    text = (name or "").strip()
    if not text:
        return ""
    stripped = _CHAPTER_PREFIX.sub("", text, count=1)
    #  剥完变空说明整个名字就是个编号，那就退回原名去掉噪声 —— 至少「第100章」
    #  和「第100章」之间还能精确比
    if not stripped.strip():
        stripped = text
    return _NOISE.sub("", stripped).lower()


#: 匹配方式。`none` 表示新目录是空的，什么都定位不了。
MATCH_EXACT = "exact"
MATCH_NORMALIZED = "normalized"
MATCH_POSITION = "position"
MATCH_NONE = "none"

#: 位置估算只在这种情况下用，必然不准。界面要据此提示用户确认。
APPROXIMATE_MATCHES = frozenset({MATCH_POSITION})


@dataclass(frozen=True)
class ChapterMatch:
    """在新目录里定位到的那一章。"""

    #: 在新目录里的下标。新目录为空时是 0。
    index: int
    #: 新目录里那一章的名字。新目录为空时是空串。
    name: str
    #: `exact` / `normalized` / `position` / `none`
    method: str
    #: 新目录的章节总数，让界面能说「第 500 / 1005 章」。
    total: int

    @property
    def is_approximate(self) -> bool:
        """估出来的位置必然不准，界面必须提示用户确认。"""
        return self.method in APPROXIMATE_MATCHES


def _proportional_index(index: int, old_total: int, new_total: int) -> int:
    """按比例把下标映射过去。

    章节数相同时退化成原下标（`round(i / (n-1) * (n-1)) == i`），所以它是「直接沿用
    下标」的严格推广 —— 数量接近时一样准，数量差很多时（有的源把章节合并了）
    明显更好。
    """
    if new_total <= 1 or old_total <= 1:
        return 0
    ratio = min(1.0, max(0.0, index / (old_total - 1)))
    return min(new_total - 1, max(0, round(ratio * (new_total - 1))))


def match_chapter(
    *,
    chapter_name: str,
    chapter_index: int,
    old_total: int,
    new_names: Sequence[str],
) -> ChapterMatch:
    """把旧源的某一章定位到新源的目录里。

    优先级：精确名 → 归一化名 → 按位置估。重名时取**离按位置估出来的位置最近**
    的那个 —— 网文里重复章名（「第一章」「番外」）很常见，取第一个命中会把读到
    后半本的人扔回开头。

    `new_names` 是新目录的章节名序列，顺序即下标。
    """
    new_total = len(new_names)
    if new_total == 0:
        return ChapterMatch(index=0, name="", method=MATCH_NONE, total=0)

    guess = _proportional_index(chapter_index, max(old_total, chapter_index + 1), new_total)

    def nearest(candidates: List[int]) -> int:
        return min(candidates, key=lambda position: (abs(position - guess), position))

    target = (chapter_name or "").strip()
    if target:
        exact = [i for i, name in enumerate(new_names) if (name or "").strip() == target]
        if exact:
            position = nearest(exact)
            return ChapterMatch(
                index=position, name=new_names[position], method=MATCH_EXACT, total=new_total
            )

        normalized = normalize_chapter_name(target)
        if normalized:
            loose = [
                i for i, name in enumerate(new_names) if normalize_chapter_name(name) == normalized
            ]
            if loose:
                position = nearest(loose)
                return ChapterMatch(
                    index=position,
                    name=new_names[position],
                    method=MATCH_NORMALIZED,
                    total=new_total,
                )

    return ChapterMatch(
        index=guess, name=new_names[guess], method=MATCH_POSITION, total=new_total
    )


def match_chapter_in(
    *,
    chapter_name: str,
    chapter_index: int,
    old_total: int,
    new_chapters: Sequence[object],
) -> ChapterMatch:
    """`match_chapter` 的便利包装，直接收 `Chapter` 对象序列。"""
    return match_chapter(
        chapter_name=chapter_name,
        chapter_index=chapter_index,
        old_total=old_total,
        new_names=[str(getattr(chapter, "name", "") or "") for chapter in new_chapters],
    )


def describe(match: ChapterMatch, source_name: str = "") -> str:
    """给界面用的一句中文说明。

    方式不同，用户要做的事不同：精确命中可以直接接着读；估出来的位置必须让用户
    确认一下 —— 所以文案里明说「可能有偏差」，而不是假装定位成功了。
    """
    where = f"「{source_name}」" if source_name else "新源"
    if match.method == MATCH_NONE:
        return f"已切换到{where}，但没能取到目录，请手动选章"
    position = f"第 {match.index + 1} / {match.total} 章"
    if match.method == MATCH_EXACT:
        return f"已切换到{where}，定位到{position}"
    if match.method == MATCH_NORMALIZED:
        return f"已切换到{where}，按章节名定位到{position}"
    return f"已切换到{where}，按位置估到{position}，可能有偏差，请确认"


__all__ = [
    "APPROXIMATE_MATCHES",
    "MATCH_EXACT",
    "MATCH_NONE",
    "MATCH_NORMALIZED",
    "MATCH_POSITION",
    "ChapterMatch",
    "describe",
    "match_chapter",
    "match_chapter_in",
    "normalize_chapter_name",
]
