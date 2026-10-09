"""换源时的章节定位。纯函数，无 IO。"""

import pytest

from funread.legado.reader.matching import (
    MATCH_EXACT,
    MATCH_NONE,
    MATCH_NORMALIZED,
    MATCH_POSITION,
    describe,
    match_chapter,
    normalize_chapter_name,
)


def names(*titles):
    return list(titles)


# ---------------------------------------------------------------- 归一化


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("第100章 惊蛰", "惊蛰"),
        ("第100章惊蛰", "惊蛰"),
        ("100.惊蛰", "惊蛰"),
        ("100、惊蛰", "惊蛰"),
        ("（100）惊蛰", "惊蛰"),
        ("【100】惊蛰", "惊蛰"),
        ("第一百章 惊蛰", "惊蛰"),
        ("第一百回 惊蛰", "惊蛰"),
        ("惊蛰", "惊蛰"),
        ("  第 100 章  惊蛰  ", "惊蛰"),
    ],
)
def test_prefix_is_stripped(raw, expected):
    """同一本书在两个源下的编号写法经常不同，不剥就永远匹配不上。"""
    assert normalize_chapter_name(raw) == expected


@pytest.mark.parametrize(
    ("a", "b"),
    [
        ("惊蛰·上", "惊蛰(上)"),
        ("惊蛰 上", "惊蛰上"),
        ("第100章 惊蛰——下", "100.惊蛰下"),
    ],
)
def test_punctuation_differences_are_ignored(a, b):
    assert normalize_chapter_name(a) == normalize_chapter_name(b)


def test_a_name_that_is_only_a_number_keeps_something_comparable():
    """整章名就是个编号时，剥完会变空 —— 那就退回原名，至少同名之间还能比。"""
    assert normalize_chapter_name("第100章") == normalize_chapter_name("第100章")
    assert normalize_chapter_name("第100章") != ""


def test_blank_name_normalizes_to_blank():
    """空串必须被调用方当作「不可比」，否则无标题章节会互相匹配上。"""
    assert normalize_chapter_name("") == ""
    assert normalize_chapter_name("   ") == ""


# ---------------------------------------------------------------- 精确命中


def test_exact_name_wins():
    match = match_chapter(
        chapter_name="第100章 惊蛰",
        chapter_index=99,
        old_total=200,
        new_names=names("楔子", "第100章 惊蛰", "第101章 春分"),
    )
    assert match.method == MATCH_EXACT
    assert match.index == 1
    assert match.name == "第100章 惊蛰"
    assert match.is_approximate is False


def test_normalized_match_when_the_numbering_style_differs():
    match = match_chapter(
        chapter_name="第100章 惊蛰",
        chapter_index=99,
        old_total=200,
        new_names=names("序", "100.惊蛰", "101.春分"),
    )
    assert match.method == MATCH_NORMALIZED
    assert match.index == 1


def test_exact_beats_normalized():
    """两者都能命中时取精确的 —— 它不依赖任何归一化假设。"""
    match = match_chapter(
        chapter_name="第100章 惊蛰",
        chapter_index=1,
        old_total=3,
        new_names=names("100.惊蛰", "第100章 惊蛰"),
    )
    assert match.method == MATCH_EXACT
    assert match.index == 1


# ---------------------------------------------------------------- 重名


def test_duplicate_names_pick_the_one_nearest_the_expected_position():
    """网文里「番外」「第一章」这种重复章名很常见。取第一个命中会把读到后半本
    的人扔回开头。"""
    new = names(*(["番外"] + [f"第{i}章" for i in range(1, 99)] + ["番外"]))
    match = match_chapter(
        chapter_name="番外", chapter_index=99, old_total=100, new_names=new
    )
    assert match.method == MATCH_EXACT
    assert match.index == 99  # 末尾那个，不是开头那个


def test_duplicate_names_near_the_start_pick_the_early_one():
    new = names(*(["番外"] + [f"第{i}章" for i in range(1, 99)] + ["番外"]))
    match = match_chapter(chapter_name="番外", chapter_index=0, old_total=100, new_names=new)
    assert match.index == 0


# ---------------------------------------------------------------- 位置估算


def test_equal_chapter_counts_fall_back_to_the_same_index():
    """按比例映射在章节数相同时退化成原下标 —— 它是「直接沿用」的严格推广。"""
    new = names(*[f"无名{i}" for i in range(1000)])
    match = match_chapter(
        chapter_name="第500章 惊蛰", chapter_index=499, old_total=1000, new_names=new
    )
    assert match.method == MATCH_POSITION
    assert match.index == 499
    assert match.is_approximate is True


def test_a_source_with_merged_chapters_maps_proportionally():
    """有的源把章节合并了（1000 → 100），直接沿用下标会越界或大错。"""
    new = names(*[f"合{i}" for i in range(100)])
    match = match_chapter(
        chapter_name="毫不相干", chapter_index=499, old_total=1000, new_names=new
    )
    #  下标 499 是第 500 章，映射到 100 章里的第 50 章 → 下标 49
    assert match.index == 49


def test_proportional_mapping_hits_both_ends_exactly():
    """首尾必须精确对上 —— 读到最后一章的人换源后不该落在倒数第二章。"""
    new = names(*[f"合{i}" for i in range(100)])
    assert match_chapter(
        chapter_name="x", chapter_index=0, old_total=1000, new_names=new
    ).index == 0
    assert match_chapter(
        chapter_name="x", chapter_index=999, old_total=1000, new_names=new
    ).index == 99


def test_position_is_clamped_to_the_new_toc():
    new = names("只有一章")
    match = match_chapter(
        chapter_name="毫不相干", chapter_index=999, old_total=1000, new_names=new
    )
    assert match.index == 0


def test_an_index_beyond_the_declared_old_total_does_not_overshoot():
    """old_total 可能是旧的（目录涨了没刷新）。不能因此算出越界的位置。"""
    new = names(*[f"x{i}" for i in range(10)])
    match = match_chapter(
        chapter_name="毫不相干", chapter_index=500, old_total=10, new_names=new
    )
    assert 0 <= match.index < 10


def test_a_blank_name_goes_straight_to_position():
    new = names("一", "二", "三", "四")
    match = match_chapter(chapter_name="", chapter_index=2, old_total=4, new_names=new)
    assert match.method == MATCH_POSITION
    assert match.index == 2


# ---------------------------------------------------------------- 空目录


def test_an_empty_new_toc_is_reported_not_guessed():
    """新源取不到目录时不能假装定位成功了。"""
    match = match_chapter(
        chapter_name="第100章", chapter_index=99, old_total=200, new_names=[]
    )
    assert match.method == MATCH_NONE
    assert match.total == 0
    assert match.index == 0


# ---------------------------------------------------------------- 文案


def test_describe_distinguishes_the_methods():
    """方式不同，用户要做的事不同 —— 文案必须说清楚是不是估的。"""
    exact = match_chapter(
        chapter_name="惊蛰", chapter_index=0, old_total=1, new_names=names("惊蛰")
    )
    assert "定位到第 1 / 1 章" in describe(exact, "乙源")
    assert "可能有偏差" not in describe(exact, "乙源")

    guessed = match_chapter(
        chapter_name="对不上", chapter_index=0, old_total=1, new_names=names("别的")
    )
    assert "可能有偏差" in describe(guessed, "乙源")

    nothing = match_chapter(chapter_name="x", chapter_index=0, old_total=1, new_names=[])
    assert "没能取到目录" in describe(nothing)


def test_describe_without_a_source_name():
    match = match_chapter(
        chapter_name="惊蛰", chapter_index=0, old_total=1, new_names=names("惊蛰")
    )
    assert "新源" in describe(match)
