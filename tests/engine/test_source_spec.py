"""`SourceSpec`：旧别名、类型漂移、采集阶段被剪掉的 base_url。"""

import pytest

from funread.legado.engine import SourceSpec, load_source

# 3.x 嵌套结构 + 已被采集流程剪掉 base_url 的 searchUrl
MODERN = {
    "bookSourceUrl": "https://example.com",
    "bookSourceName": "示例源",
    "bookSourceType": 0,
    "searchUrl": "/search?q={{key}}&p={{page}}",
    "ruleSearch": {
        "bookList": "class.result@tag.li",
        "name": "tag.a@text",
        "bookUrl": "tag.a@href",
        "author": "class.author@text",
    },
    "ruleBookInfo": {"intro": "id.intro@text", "tocUrl": "text.目录@href"},
    "ruleToc": {
        "chapterList": "id.list@tag.a",
        "chapterName": "text",
        "chapterUrl": "href",
    },
    "ruleContent": {"content": "id.content@html"},
}

# 2.x 扁平字段名
LEGACY = {
    "bookSourceUrl": "https://old.example.com/",
    "bookSourceName": "老源",
    "searchUrl": "https://old.example.com/s?wd={{key}}",
    "ruleSearchList": "class.box@tag.dl",
    "ruleSearchName": "tag.dt@text",
    "ruleSearchNoteUrl": "tag.dt@tag.a@href",
    "ruleChapterList": "id.chapters@tag.a",
    "ruleChapterName": "text",
    "ruleChapterUrl": "href",
    "ruleBookContent": "id.booktext@html",
}


def test_modern_structure_passthrough():
    spec = load_source(MODERN)
    assert spec.url == "https://example.com"
    assert spec.name == "示例源"
    assert spec.rule("ruleSearch", "bookList") == "class.result@tag.li"
    assert spec.rule("ruleContent", "content") == "id.content@html"
    assert spec.is_complete
    assert spec.missing_core_fields() == []
    assert spec.needs_js() is False


def test_relative_search_url_is_rejoined():
    """采集阶段把 base_url 从 searchUrl 里剪掉了，必须拼回去才发得出请求。"""
    spec = load_source(MODERN)
    assert spec.absolute(spec.search_url) == ("https://example.com/search?q={{key}}&p={{page}}")


def test_absolute_search_url_left_alone():
    spec = load_source(LEGACY)
    assert spec.absolute(spec.search_url) == "https://old.example.com/s?wd={{key}}"


def test_absolute_handles_missing_leading_slash():
    spec = load_source(MODERN)
    assert spec.absolute("search?q=1") == "https://example.com/search?q=1"
    assert spec.absolute("") == ""


def test_absolute_keeps_port_and_query_leftovers_glued():
    """剪 base_url 时把端口/查询串的分隔符一起留下了，中间不能再补斜杠。

    实测 5sing 的 searchUrl 就是 `:80/home/json?...`（原本是
    `http://search.5sing.kugou.com:80/home/json?...`），补一道斜杠就成了
    `/:80/home/json`，必 404。
    """
    spec = load_source(MODERN)
    assert spec.absolute(":80/home/json?k=1") == "https://example.com:80/home/json?k=1"
    assert spec.absolute("?k=1") == "https://example.com?k=1"


def test_absolute_protocol_relative_url():
    spec = load_source(MODERN)
    assert spec.absolute("//cdn.other.com/s?q=1") == "https://cdn.other.com/s?q=1"


def test_source_url_truncated_at_hash():
    spec = load_source({"bookSourceUrl": "https://example.com/#🎃", "bookSourceName": "x"})
    assert spec.url == "https://example.com"


def test_trailing_slash_stripped():
    assert load_source({"bookSourceUrl": "https://x.com/"}).url == "https://x.com"


# ------------------------------------------------------------------ 旧别名


def test_legacy_flat_names_mapped():
    spec = load_source(LEGACY)
    assert spec.rule("ruleSearch", "bookList") == "class.box@tag.dl"
    assert spec.rule("ruleToc", "chapterList") == "id.chapters@tag.a"


def test_legacy_note_url_is_book_url():
    """2.x 的 `noteUrl` 就是 3.x 的 `bookUrl`，不认的话搜索结果点不进去。"""
    spec = load_source(
        {
            "bookSourceUrl": "https://x.com",
            "ruleSearch": {"bookList": "a", "name": "b", "noteUrl": "tag.a@href"},
        }
    )
    assert spec.rule("ruleSearch", "bookUrl") == "tag.a@href"


def test_top_level_group_aliases():
    spec = load_source(
        {
            "bookSourceUrl": "https://x.com",
            "searchRule": {"bookList": "l", "name": "n", "bookUrl": "u"},
            "catalogRule": {"chapterList": "cl", "chapterName": "cn", "chapterUrl": "cu"},
            "contentRule": {"content": "c"},
            "searchUrl": "/s",
        }
    )
    assert spec.rule("ruleSearch", "bookList") == "l"
    assert spec.rule("ruleToc", "chapterList") == "cl"
    assert spec.rule("ruleContent", "content") == "c"
    assert spec.is_complete


def test_next_url_aliases():
    spec = load_source(
        {
            "bookSourceUrl": "https://x.com",
            "ruleToc": {"chapterUrlNext": "id.next@href"},
            "ruleContent": {"urlNext": "id.cnext@href"},
        }
    )
    assert spec.rule("ruleToc", "nextTocUrl") == "id.next@href"
    assert spec.rule("ruleContent", "nextContentUrl") == "id.cnext@href"


def test_rule_book_content_moved_to_content_rule():
    """采集侧把 `ruleBookContent` 误塞进 `ruleBookInfo.content`，按 Legado 语义搬回。

    `ruleBookContent` 是**章节正文**规则，不是详情页字段。不纠正的话
    `ruleContent.content` 为空，这个源会被判成「不完整」而被直接跳过。
    """
    spec = load_source(LEGACY)
    assert spec.rule("ruleContent", "content") == "id.booktext@html"
    assert spec.rule("ruleBookInfo", "content") == ""
    assert spec.is_complete


def test_existing_content_rule_not_overwritten():
    spec = load_source(
        {
            "bookSourceUrl": "https://x.com",
            "ruleBookInfo": {"content": "错的", "intro": "i"},
            "ruleContent": {"content": "对的"},
        }
    )
    assert spec.rule("ruleContent", "content") == "对的"
    assert spec.rule("ruleBookInfo", "intro") == "i"


# ------------------------------------------------------------------ 类型漂移


@pytest.mark.parametrize("value,expected", [(0, 0), ("0", 0), (1, 1), ("3", 3), (None, 0), ("", 0)])
def test_book_source_type_accepts_int_or_str(value, expected):
    spec = load_source({"bookSourceUrl": "https://x.com", "bookSourceType": value})
    assert spec.book_source_type == expected


def test_header_as_dict():
    spec = load_source({"bookSourceUrl": "https://x.com", "header": {"User-Agent": "UA"}})
    assert spec.header == {"User-Agent": "UA"}


def test_header_as_json_string():
    spec = load_source({"bookSourceUrl": "https://x.com", "header": '{"Referer": "https://x.com"}'})
    assert spec.header == {"Referer": "https://x.com"}


def test_header_as_bare_user_agent():
    """`httpUserAgent` 迁移过来的裸 UA 字符串。"""
    spec = load_source({"bookSourceUrl": "https://x.com", "httpUserAgent": "Mozilla/5.0"})
    assert spec.header == {"User-Agent": "Mozilla/5.0"}


def test_header_broken_json_falls_back_to_ua():
    spec = load_source({"bookSourceUrl": "https://x.com", "header": "{not json"})
    assert spec.header == {"User-Agent": "{not json"}


def test_rule_group_as_plain_string():
    """`ruleContent` 偶尔整体就是一条规则串。"""
    spec = load_source({"bookSourceUrl": "https://x.com", "ruleContent": "id.content@html"})
    assert spec.rule("ruleContent", "content") == "id.content@html"


def test_rule_value_as_list_becomes_or_chain():
    """多条候选规则等价于 `||`。"""
    spec = load_source(
        {"bookSourceUrl": "https://x.com", "ruleContent": {"content": ["a@text", "b@text"]}}
    )
    assert spec.rule("ruleContent", "content") == "a@text||b@text"


def test_rule_value_as_int():
    spec = load_source({"bookSourceUrl": "https://x.com", "ruleToc": {"chapterName": 3}})
    assert spec.rule("ruleToc", "chapterName") == "3"


# -------------------------------------------------------------- 包装与体检


def wrapped(*, merged=None, candidate=None):
    """`hubs/book/source/<bucket>/<url_id>.json` 的真实形状。

    `final` 是**布尔标记**，不是源对象；源在 `merged`/`candidate` 的
    `{md5_list, source}` 里。
    """
    return {
        "url_id": 42,
        "hostname": "example.com",
        "status": 2,
        "available": True,
        "final": False,
        "merged": [{"md5_list": ["m"], "source": s} for s in (merged or [])],
        "candidate": [{"md5_list": ["c"], "source": s} for s in (candidate or [])],
    }


def test_candidate_source_unwrapped():
    spec = load_source(wrapped(candidate=[dict(MODERN)]))
    assert spec.url == "https://example.com"
    assert spec.is_complete
    assert "url_id" not in spec.raw
    assert "candidate" not in spec.raw


def test_merged_preferred_over_candidate():
    """`merged` 是合并流程挑出来的结果，比原始候选可信。"""
    merged = dict(MODERN, bookSourceName="合并版")
    candidate = dict(MODERN, bookSourceName="候选版")
    assert load_source(wrapped(merged=[merged], candidate=[candidate])).name == "合并版"


def test_most_complete_candidate_chosen():
    """同一 url_id 常有上百个历史变体，挑字段最全的那个。"""
    shell = {"bookSourceUrl": "https://example.com", "bookSourceName": "空壳版"}
    full = dict(MODERN, bookSourceName="完整版")
    spec = load_source(wrapped(candidate=[shell, full]))
    assert spec.name == "完整版"
    assert spec.is_complete


def test_final_flag_is_not_a_source():
    """回归：`final` 曾被当成源对象去取，结果每个源都解析成空壳。"""
    spec = load_source(wrapped(candidate=[dict(MODERN)]))
    assert spec.is_complete
    assert spec.raw.get("final") is None


def test_bare_source_passes_through_wrapper_path():
    """没有 merged/candidate 的裸源不该被动到。"""
    spec = load_source(dict(MODERN, url_id=7, hostname="example.com"))
    assert spec.is_complete
    assert "url_id" not in spec.raw


def test_single_source_wrapper():
    spec = load_source({"url_id": 1, "source": dict(MODERN)})
    assert spec.is_complete


def test_empty_shell_reports_all_missing():
    """实测 19% 的代表源是空壳，选源前必须靠这个过滤掉。"""
    spec = load_source({"bookSourceUrl": "https://x.com", "bookSourceName": "空壳"})
    assert not spec.is_complete
    assert set(spec.missing_core_fields()) == {
        "searchUrl",
        "ruleSearch.bookList",
        "ruleSearch.name",
        "ruleSearch.bookUrl",
        "ruleToc.chapterList",
        "ruleToc.chapterName",
        "ruleToc.chapterUrl",
        "ruleContent.content",
    }


def test_partial_source_reports_only_missing():
    data = dict(MODERN)
    data["ruleContent"] = {}
    spec = load_source(data)
    assert spec.missing_core_fields() == ["ruleContent.content"]


def test_needs_js_detected_statically():
    data = dict(MODERN)
    data["searchUrl"] = "/s?t={{java.timeFormat()}}"
    assert load_source(data).needs_js() is True


def test_placeholder_only_source_is_js_free():
    assert load_source(MODERN).needs_js() is False


# ------------------------------------------------------------ 发现页分类


def test_explore_named_urls():
    spec = load_source(
        {
            "bookSourceUrl": "https://x.com",
            "exploreUrl": "玄幻::/list/1\n都市::/list/2",
        }
    )
    kinds = spec.explore_kinds()
    assert [(k.name, k.url) for k in kinds] == [("玄幻", "/list/1"), ("都市", "/list/2")]


def test_explore_json_array():
    spec = load_source(
        {
            "bookSourceUrl": "https://x.com",
            "exploreUrl": '[{"title":"热门","url":"/hot"}]',
        }
    )
    assert [(k.name, k.url) for k in spec.explore_kinds()] == [("热门", "/hot")]


def test_explore_empty():
    assert load_source({"bookSourceUrl": "https://x.com"}).explore_kinds() == []


def test_search_urls_single_line():
    spec = load_source(MODERN)
    assert spec.search_urls == ["/search?q={{key}}&p={{page}}"]


def test_from_dict_keeps_raw():
    spec = SourceSpec.from_dict(MODERN)
    assert spec.raw["bookSourceName"] == "示例源"
