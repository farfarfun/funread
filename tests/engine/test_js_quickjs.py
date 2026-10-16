"""quickjs 运行时与 `java.*` 宿主桥。

这些用例的选型依据是实测频率：扫了归档里 13,264 个书源，6,702 个含 JS，一共
209 个不同的宿主方法，而**前 10 个覆盖 75.8% 的调用**。所以这里优先钉高频的那批，
不追求把 Legado 文档上的方法测全。
"""

import pytest

from funread.legado.engine.errors import JsNotSupportedError
from funread.legado.engine.js import default_js_runtime
from funread.legado.engine.js.base import JsContext
from funread.legado.engine.js.host import cn_to_int
from funread.legado.engine.variables import VariableScope

quickjs = pytest.importorskip("quickjs", reason="需要 funread[js]")


@pytest.fixture
def runtime():
    return default_js_runtime("quickjs")


@pytest.fixture
def context():
    return JsContext(
        base_url="https://example.com/book/",
        result="第一百二十三章 测试",
        variables=VariableScope(),
    )


# ------------------------------------------------------------------ 原生 String


def test_native_string_methods_need_no_host_binding(runtime, context):
    """频率榜第 2 和第 8 是 `result.match` 和 `result.replace`。

    它们是 JS 原生的 String 方法，只要 `result` 作为字符串注入就自动可用 ——
    把它们当宿主 API 实现是白做功。这条钉住「注入的确实是字符串」。
    """
    assert runtime.eval("result.match(/第(.+?)章/)[1]", context) == "一百二十三"
    assert runtime.eval('result.replace("测试", "OK")', context) == "第一百二十三章 OK"
    assert runtime.eval('result.split(" ")[1]', context) == "测试"


# ------------------------------------------------------------------ 高频宿主 API


def test_md5_encode(runtime, context):
    """频率第 1，173,915 次调用。"""
    assert runtime.eval('java.md5Encode("abc")', context) == ("900150983cd24fb0d6963f7d28e17f72")


def test_md5_encode16_is_the_middle_of_the_32(runtime, context):
    """Legado 的 16 位 md5 不是另一种算法，就是 32 位取中间 16 位。"""
    full = runtime.eval('java.md5Encode("abc")', context)
    assert runtime.eval('java.md5Encode16("abc")', context) == full[8:24]


def test_variables_round_trip(runtime, context):
    """`java.put` / `java.get` 是频率第 3 和第 7，共 24 万次调用。

    `put` 要**返回写入的值** —— 规则里常直接把它当表达式结果用。
    """
    assert runtime.eval('java.put("token", "abc")', context) == "abc"
    assert runtime.eval('java.get("token")', context) == "abc"
    #  写进去的东西要落到传进来的作用域上，而不是只活在这次求值里
    assert context.variables.get("token") == "abc"


def test_base64_roundtrip_and_missing_padding(runtime, context):
    assert runtime.eval('java.base64Encode("hello")', context) == "aGVsbG8="
    assert runtime.eval('java.base64Decode("aGVsbG8=")', context) == "hello"
    #  书源里的 base64 常常缺尾部填充，严格解码会直接抛
    assert runtime.eval('java.base64Decode("aGVsbG8")', context) == "hello"


def test_to_num_chapter(runtime, context):
    assert runtime.eval("java.toNumChapter(result)", context) == "第123章 测试"


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("一", 1),
        ("十", 10),
        ("十二", 12),  # 省略前导一
        ("二十", 20),
        ("一百二十三", 123),
        ("一百零八", 108),
        ("not-chinese", None),
        ("", None),
    ],
)
def test_cn_to_int(text, expected):
    assert cn_to_int(text) == expected


def test_source_key_falls_back_to_base_url(runtime, context):
    """`source.getKey` 频率第 9。没有源信息时回落到 baseUrl，而不是空串 ——
    规则常拿它当缓存键，空串会让所有源共用一个键。"""
    assert runtime.eval("source.getKey()", context) == "https://example.com/book/"


def test_injected_variables(runtime, context):
    assert runtime.eval("baseUrl", context) == "https://example.com/book/"
    assert runtime.eval("page", context) == 1


# ------------------------------------------------------------------ 不该抛的地方


def test_toast_is_not_an_error(runtime, context):
    """原生 APP 弹吐司，服务端没界面。

    **不能抛**：规则作者常拿它当调试输出，抛错会让一堆本来能跑的源挂掉。
    """
    assert runtime.eval('java.toast("hi")', context) == "hi"


def test_login_info_map_is_empty_not_an_error(runtime, context):
    """我们不支持书源登录。

    规则里典型写法是 `source.getLoginInfoMap()?.get("x")`，拿不到就走未登录
    分支 —— 返回空表让这条分支正常工作，抛错反而把整个源判死。
    """
    assert runtime.eval("JSON.stringify(source.getLoginInfoMap())", context) == "{}"


def test_ajax_without_a_fetcher_degrades_to_empty(runtime, context):
    """没注入 fetcher 时 `java.ajax` 返回空串而不是炸穿 JS 边界。

    宿主异常穿过 quickjs 的 C 栈往上抛，行为取决于绑定实现，最糟是整个解释器
    状态不可用。折成空串让规则走自己的失败分支 —— 它本来就得处理「接口没返回」。
    """
    assert runtime.eval('java.ajax("https://example.com")', context) == ""


# ------------------------------------------------------------------ 资源上限


def test_runaway_string_growth_is_stopped(runtime, context):
    """失控拼接必须被内存上限挡住，而不是吃光机器。"""
    with pytest.raises(JsNotSupportedError, match="JS 执行失败"):
        runtime.eval('var s = "x"; while (true) { s += s; }', context)


def test_infinite_recursion_is_stopped(runtime, context):
    with pytest.raises(JsNotSupportedError, match="JS 执行失败"):
        runtime.eval("function r() { return r(); } r();", context)


def test_host_calls_still_work_after_a_limit_was_hit(runtime, context):
    """撞上限之后，下一次求值必须是干净的。

    每次 eval 新建 Context 就是为了这个 —— 顺便也隔开了规则之间的全局变量污染，
    而规则作者大量使用裸赋值（`x = 1`），复用 Context 会让源之间串台。
    """
    with pytest.raises(JsNotSupportedError):
        runtime.eval('var s = "x"; while (true) { s += s; }', context)
    assert runtime.eval('java.md5Encode("abc")', context).startswith("90015098")


def test_syntax_error_becomes_a_rule_error(runtime, context):
    """语法错的规则要变成 JsNotSupportedError，而不是漏一个 quickjs 的内部异常
    出去 —— 上层按 RuleError 分类处理，漏出去的会被当成未知错误吞掉堆栈。"""
    with pytest.raises(JsNotSupportedError):
        runtime.eval("this is not javascript", context)


# ------------------------------------------------------------------ 语句块形态


def test_a_statement_block_with_return_works(runtime, context):
    """Legado 的规则大多是表达式，但也有带 `return` 的语句块。顶层 `return` 在
    `eval` 里是语法错，所以要包一层函数。"""
    script = 'var x = java.md5Encode("a"); return x.length;'
    assert runtime.eval(script, context) == 32


# ------------------------------------------------------------------ 绑定的类型约束


def test_byte_array_round_trips_through_latin1(runtime, context):
    """quickjs 的绑定只能返回 str/int/float/bool/None —— list、dict、bytes 一律
    "Can not convert Python result to JS"。

    所以字节用 latin-1 字符串承载：它是唯一字节值与码点一一对应的编码，往返无损，
    而且 JS 侧的 `.length` 正好是字节数。规则对这个返回值的用法几乎全是「再喂回
    另一个 java.*」（AES 的 key/iv），那条路上用 `as_bytes()` 还原。
    """
    from funread.legado.engine.js.host import as_bytes

    #  "aGk=" 解出来是 b"hi"，两个字节
    assert runtime.eval('java.base64DecodeToByteArray("aGk=").length', context) == 2
    #  非 ASCII 字节也要无损：0xFF 在 utf-8 下会被编成两个字节，latin-1 不会
    raw = runtime.eval('java.base64DecodeToByteArray("/w==")', context)
    assert as_bytes(raw) == b"\xff"


def test_get_elements_comes_back_as_a_real_array(runtime, context):
    """同样因为返回不了 list，Python 侧给 JSON 字符串、bootstrap 里 parse 回数组。

    绕 JSON 而不是改成「返回长度 + 逐个取」，是因为规则对结果做的是任意数组操作
    （遍历、slice、map），只有真数组才都能用。
    """
    runtime_with_select = default_js_runtime(
        "quickjs", select=lambda html, rule, want_list: ["a", "b", "c"]
    )
    assert runtime_with_select.eval("java.getElements('x').length", context) == 3
    assert runtime_with_select.eval("java.getElements('x')[1]", context) == "b"
    assert runtime_with_select.eval("java.getElement('x')", context) == "a"


# ------------------------------------------------------------------ 脚本形态


def test_multi_statement_script_returns_its_last_expression(runtime, context):
    """quickjs 的 eval 本来就支持多语句，不要替它包装。"""
    assert runtime.eval("var x = 1; x + 2", context) == 3


def test_a_string_containing_the_word_return_is_not_a_statement_block(runtime, context):
    """这条是回归：判断脚本形态**不能**用 `"return" in body` 做判据。

    `var s = "returned"; s` 里的 return 只是字符串的一部分。早先按它分支，导致
    40 个 needs_js 的源里有 7 个报 SyntaxError —— 而且报的像是源的规则有问题，
    不像我们的 bug，极难定位。
    """
    assert runtime.eval('var s = "returned"; s', context) == "returned"


def test_top_level_return_is_wrapped(runtime, context):
    """唯一需要兜底的形态：顶层 return 在 program 模式下是语法错。"""
    assert runtime.eval("return 42;", context) == 42
    assert runtime.eval('var x = 1; if (x) { return "yes"; } return "no";', context) == "yes"


def test_a_failed_host_call_is_named_in_the_error(runtime, context):
    """宿主调用失败必须出现在最终的错误信息里。

    这条是血泪回归。`java.ajax` 连不上时降级成空串，规则下一行
    `JSON.parse(ajax_result)` 就会抛 `SyntaxError: unexpected end of input` ——
    看起来像这段 JS 有语法错，而真正的原因是上一行没连上网。诊断归档源时真的
    被它骗过一次，花了不少时间去查「JS 片段是不是提取时被截断了」。

    降级本身是对的（异常穿过 quickjs 的 C 栈很危险），但必须留痕。
    """

    def boom(url: str) -> str:
        raise ConnectionError("连不上")

    rt = default_js_runtime("quickjs", fetcher=None)
    #  直接塞一个会炸的 ajax：比起真发网络请求，这样测得准也快
    rt._make_ajax = lambda: boom  # type: ignore[method-assign]

    with pytest.raises(JsNotSupportedError) as caught:
        rt.eval('JSON.parse(java.ajax("https://nope.invalid"))', context)

    message = str(caught.value)
    #  既要有 JS 报的那个误导性错误，也要有真正的原因
    assert "unexpected end of input" in message
    assert "宿主调用失败" in message
    assert "ajax" in message
    assert "ConnectionError" in message
