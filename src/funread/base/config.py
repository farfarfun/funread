"""配置解析：**显式传参 > config.toml > funsecret > 兜底**。

funread 全家的每一个设置项都走这一条链，没有例外，也**不读任何 `FUNREAD_*`
环境变量**。环境变量这一层在服务化部署里是最难追的 —— 它不在任何文件里，systemd
单元、shell profile、部署脚本都可能是来源，出问题时现场无从复原。原先靠环境变量
配的东西现在一律写进 config.toml。

四层分别是：

1. **显式传参** —— :func:`set_overrides` / :func:`override`。命令行开关、测试夹具、
   一次性脚本用它。进程内的东西，不落盘。
2. **config.toml** —— :func:`use_config_file` 注册的文件（服务自己那份，如
   ``~/.farfarfun/funread/api/config.toml``）按注册顺序优先，最后兜到共享根文件
   :func:`shared_config_path`（``~/.farfarfun/funread/config.toml``）。
3. **funsecret** —— 凭据的推荐位置。config.toml 也能放凭据，但密钥库里的那份不必
   落到每台机器的磁盘上。**只放换台机器照样成立的值**：库地址、口令属于这一类，
   本地路径不属于 —— 密钥库是跨机器共享的，一台机器写进去，另一台就读到别人的路径。
   :func:`resolve_cache_root` 因此不走这一层，理由写在它的 docstring 里。
4. **兜底** —— 本地 SQLite / 用户缓存目录。让一份新 clone 在没有任何密钥库、没有任何
   配置文件的情况下也能把管线和 API 跑起来。

第 3 层的"没配"和"坏掉"必须分开：前者才兜底，后者原样抛。一个坏掉的密钥库如果被
默默兜底，运维会以为自己在写配置里那个库，其实在写本地 SQLite。
"""

from __future__ import annotations

import json
import os
import threading
import tomllib
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from farlog import getLogger

logger = getLogger("funread")

#: 共享根配置文件相对 HOME 的位置。三个入口（核心库管线、funread-api、funread-web）
#: 都读它，所以库地址、缓存目录这类跨进程的设置只需要写一处。**故意不走
#: XDG_CONFIG_HOME**：同一个目录树下还放 PID、日志、会话密钥，那些都不是"配置"。
SHARED_CONFIG_RELATIVE = Path(".farfarfun") / "funread" / "config.toml"

#: Fixed under the user cache dir (not CWD-relative) so every entrypoint
#: (CLI pipeline, API server, scripts) reads/writes the same file regardless
#: of where it's invoked from.
DEFAULT_DATABASE_PATH = Path.home() / ".cache" / "farfarfun" / "funread" / "funread.db"
DEFAULT_DATABASE_URL = f"sqlite:///{DEFAULT_DATABASE_PATH}"

#: Where downloaded source JSON files are archived. Same reasoning as the
#: database path: fixed under the user cache dir, never CWD-relative.
DEFAULT_CACHE_ROOT = Path.home() / ".cache" / "farfarfun" / "funread" / "hub"

#: 核心库自己那两个已废弃的环境变量 -> 现在该写哪个键。入口进程把自己的表并上去，
#: 一起交给 :func:`warn_ignored_env`。
DEPRECATED_ENV_VARS = {
    "FUNREAD_DATABASE_URL": "config.toml 的 [database] url",
    "FUNREAD_CACHE_ROOT": "config.toml 的 [cache] root",
}

#: 配置文件支持的三种扩展名。``.env`` 里只能写扁平键（``database_url=...``，
#: 可带 ``FUNREAD_`` 前缀），没有段的概念。
_SUPPORTED_SUFFIXES = (".toml", ".json", ".env")

#: Errors that mean "no secret configured here", as opposed to "the secret
#: store malfunctioned". Only the former falls back -- a broken store must stay
#: loud, otherwise a misconfigured host silently writes to local SQLite while
#: the operator believes it is on the configured database.
_SECRET_ABSENT = (ImportError, KeyError, ValueError)

#: 一个配置项在文件里可能出现的位置，形如 ``(段名 or None, 键名)``。
Candidate = tuple[str | None, str]


class _Missing:
    """ "这一项根本没出现"的标记。不能用 ``None``：配置里显式写 ``None`` 是合法的。"""

    def __repr__(self) -> str:  # pragma: no cover - 只为报错好看
        return "<missing>"


_MISSING = _Missing()

_LOCK = threading.RLock()
_overrides: dict[str, Any] = {}
_config_files: list[Path] = []
_revision = 0
#: path -> ((mtime_ns, size), 解析结果)。不带缓存的话每次 ``resolve_*`` 都读盘，
#: 而 API 的每个请求都会走到这里。
_document_cache: dict[Path, tuple[tuple[int, int], dict[str, Any]]] = {}
#: 「密钥库里还压着废弃的 cache_root」这条警告只发一次，见
#: :func:`_warn_legacy_cache_root_secret`。
_LEGACY_CACHE_ROOT_WARNED = False


# ---------------------------------------------------------------------------
# 第 1 层：显式传参
# ---------------------------------------------------------------------------


def config_revision() -> int:
    """每次 override / 文件注册变动就 +1。

    给调用方做缓存键用：funread-api 的 service 容器按它判断"配置变没变"，比每个
    请求都重新解析一遍（会打 funsecret）便宜得多。
    """
    return _revision


def _bump() -> None:
    global _revision
    _revision += 1
    # 清缓存而不是等 mtime 变：override/注册只发生在启动和测试边界上，清掉不心疼，
    # 而留着会让"同一纳秒内重写配置文件"的测试偶发地读到旧内容。
    _document_cache.clear()


def set_overrides(**values: Any) -> None:
    """设置进程内覆盖值。``None`` 表示删掉这一项，不是"覆盖成 None"。"""
    with _LOCK:
        for name, value in values.items():
            if value is None:
                _overrides.pop(name, None)
            else:
                _overrides[name] = value
        _bump()


def clear_overrides() -> None:
    with _LOCK:
        _overrides.clear()
        _bump()


@contextmanager
def override(**values: Any) -> Iterator[None]:
    """临时覆盖，退出时精确还原（包括"原来没有这一项"）。"""
    with _LOCK:
        previous = {name: _overrides.get(name, _MISSING) for name in values}
    try:
        set_overrides(**values)
        yield
    finally:
        with _LOCK:
            for name, value in previous.items():
                if value is _MISSING:
                    _overrides.pop(name, None)
                else:
                    _overrides[name] = value
            _bump()


# ---------------------------------------------------------------------------
# 第 2 层：config.toml
# ---------------------------------------------------------------------------


def use_config_file(path: str | Path) -> None:
    """把一份配置文件插到共享根文件前面。

    服务进程解析出自己的 config 路径之后调一次，这样 ``[auth]``、``[database]``
    这些段既可以写在服务自己那份里，也可以写在共享根文件里，前者优先。
    重复注册同一个路径是空操作。
    """
    resolved = Path(path).expanduser()
    with _LOCK:
        if resolved in _config_files:
            return
        _config_files.append(resolved)
        _bump()


def reset_config_files() -> None:
    with _LOCK:
        _config_files.clear()
        _bump()


def shared_config_path() -> Path:
    """共享根配置文件的绝对路径。

    每次调用重新算，**不在 import 期定下来**：两个仓库的测试都靠 `monkeypatch`
    改 `HOME` 来隔离真机配置，import 期的常量会让那套隔离整个失效 —— 测试会读到
    开发机上真实的那份 config.toml。
    """
    return Path.home() / SHARED_CONFIG_RELATIVE


def config_files() -> list[Path]:
    """实际的查找顺序：注册的文件（先注册先优先），最后共享根文件。"""
    with _LOCK:
        registered = list(_config_files)
    shared = shared_config_path()
    if shared not in registered:
        registered.append(shared)
    return registered


def _normalise_env_document(raw: dict[str, str]) -> dict[str, Any]:
    """``.env`` 的键折成扁平设置名：``FUNREAD_DATABASE_URL`` -> ``database_url``。"""
    return {str(key).strip().lower().removeprefix("funread_"): value for key, value in raw.items()}


def _parse_env(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key.strip()] = value
    return values


def _parse_document(path: Path, text: str) -> dict[str, Any]:
    suffix = path.suffix.lower()
    try:
        if suffix == ".toml":
            raw: Any = tomllib.loads(text)
        elif suffix == ".json":
            raw = json.loads(text)
        elif suffix == ".env":
            raw = _normalise_env_document(_parse_env(text))
        else:
            raise RuntimeError(
                f"unsupported config extension {suffix or '(none)'}: "
                f"expected one of {', '.join(_SUPPORTED_SUFFIXES)}"
            )
    except (tomllib.TOMLDecodeError, json.JSONDecodeError) as error:
        raise RuntimeError(f"cannot parse config file {path}: {error}") from None
    if not isinstance(raw, dict):
        raise RuntimeError(f"config file {path} must contain a mapping at the top level")
    return raw


def load_document(path: str | Path) -> dict[str, Any]:
    """读并解析一份配置文件；文件不存在返回 ``{}``。

    不存在不是错误 —— 新装的机器上共享根文件本来就没有。**解析失败是错误**：一个
    写坏了的 config.toml 如果被当成空文件，整台机器会静默跑回兜底配置。
    """
    resolved = Path(path).expanduser()
    try:
        stat = resolved.stat()
    except FileNotFoundError:
        return {}
    except OSError as error:
        raise RuntimeError(f"cannot read config file {resolved}: {error}") from None

    signature = (stat.st_mtime_ns, stat.st_size)
    cached = _document_cache.get(resolved)
    if cached is not None and cached[0] == signature:
        return cached[1]

    try:
        text = resolved.read_text(encoding="utf-8")
    except OSError as error:
        raise RuntimeError(f"cannot read config file {resolved}: {error}") from None

    document = _parse_document(resolved, text)
    _document_cache[resolved] = (signature, document)
    return document


def _lookup(document: dict[str, Any], candidates: tuple[Candidate, ...]) -> Any:
    for section, key in candidates:
        if section is None:
            if key in document:
                return document[key]
            continue
        table = document.get(section)
        if isinstance(table, dict) and key in table:
            return table[key]
    return _MISSING


def config_value_with_origin(*candidates: Candidate) -> tuple[Any, Path | None]:
    """同 :func:`config_value`，另外告诉调用方这个值出自哪份文件。

    给报错文案用：值可能来自服务自己那份 config.toml，也可能来自共享根文件，报错时
    说错文件名会把人引到一个根本没有这个键的文件上去。找不到时返回 ``(None, None)``。
    """
    for path in config_files():
        value = _lookup(load_document(path), candidates)
        if value is not _MISSING:
            return value, path
    return None, None


def config_value(*candidates: Candidate) -> Any:
    """按文件顺序、每份文件里按 ``candidates`` 顺序取第一个命中的值。

    候选位置由调用方点名，没有任何猜测：``("database", "url")`` 是 ``[database]``
    段里的 ``url``，``(None, "database_url")`` 是顶层的 ``database_url``（``.env``
    这种没有段的格式靠它）。找不到返回 ``None``。
    """
    return config_value_with_origin(*candidates)[0]


# ---------------------------------------------------------------------------
# 第 3 层：funsecret
# ---------------------------------------------------------------------------


def read_secret_value(*, cate2: str, cate3: str, cate4: str) -> Any:
    """读 funsecret ``funread/<cate2>/<cate3>/<cate4>``；没配返回 ``None``。"""
    try:
        from funsecret import read_secret

        return read_secret(cate1="funread", cate2=cate2, cate3=cate3, cate4=cate4)
    except _SECRET_ABSENT as exc:
        logger.debug(f"funread/{cate2}/{cate3}/{cate4} not configured in funsecret: {exc}")
        return None


# ---------------------------------------------------------------------------
# 四层合一
# ---------------------------------------------------------------------------


def resolve_setting(
    name: str,
    *,
    candidates: tuple[Candidate, ...],
    secret: tuple[str, str, str] | None = None,
    fallback: Any = None,
    coerce: Callable[[Any], Any] | None = None,
) -> Any:
    """唯一的链条实现：override > config.toml > funsecret > 兜底。

    :param name: override 里用的扁平设置名。
    :param candidates: 在配置文件里的候选位置，见 :func:`config_value`。
    :param secret: funsecret 坐标 ``(cate2, cate3, cate4)``；``None`` 表示这一项
        不该进密钥库（监听地址之类）。
    :param fallback: 兜底值，或一个无参 callable（需要建目录之类的副作用时用）。
    :param coerce: 对**非兜底**的命中值做类型转换。config.toml 给的是真正的 TOML
        类型，funsecret 给的永远是字符串，两边得折到一处。
    """
    with _LOCK:
        if name in _overrides:
            return _coerce(_overrides[name], coerce)

    value = config_value(*candidates)
    if value is not None and value != "":
        return _coerce(value, coerce)

    if secret is not None:
        cate2, cate3, cate4 = secret
        value = read_secret_value(cate2=cate2, cate3=cate3, cate4=cate4)
        if value is not None and value != "":
            return _coerce(value, coerce)

    return fallback() if callable(fallback) else fallback


def _coerce(value: Any, coerce: Callable[[Any], Any] | None) -> Any:
    return value if coerce is None else coerce(value)


def as_bool(value: Any) -> bool:
    """TOML 的真 bool 和 funsecret 的字符串折到一处。"""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def ignored_env_vars(mapping: dict[str, str]) -> list[str]:
    """已废弃的 `FUNREAD_*` 里哪些还被设着（空白值不算）。

    只检测、不喊。喊法交给各个入口：CLI 要打到 stderr 让人当场看见，服务进程要进
    自己的日志。库这一层去挑 logger 的话，在 CLI 那条路上大概率谁都看不到。

    必须有这个检测：不留兼容读取、又不说一声的话，升级会静默改变行为 —— 原先靠
    `FUNREAD_DATABASE_URL` 指到某个库的机器，升级后会一路掉到 funsecret 去。
    """
    return [name for name in mapping if os.environ.get(name, "").strip()]


def describe_ignored_env(mapping: dict[str, str]) -> list[str]:
    """把 :func:`ignored_env_vars` 的结果变成可以直接打出去的人话。"""
    return [
        f"环境变量 {name} 已不再被读取，请改写 {mapping[name]}"
        for name in ignored_env_vars(mapping)
    ]


# ---------------------------------------------------------------------------
# 具体设置项
# ---------------------------------------------------------------------------


def _fallback_to_default() -> str:
    DEFAULT_DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    return DEFAULT_DATABASE_URL


def _fallback_cache_root() -> str:
    """兜底到用户缓存目录，顺带检查密钥库里是否还压着那条废弃的路径。

    只在兜底这条路上查密钥库：配了 ``[cache] root`` 的机器一次都不会查。
    """
    _warn_legacy_cache_root_secret()
    DEFAULT_CACHE_ROOT.mkdir(parents=True, exist_ok=True)
    return str(DEFAULT_CACHE_ROOT)


def _warn_legacy_cache_root_secret() -> None:
    """密钥库里还留着 ``funread/cache/path/root`` 就喊一声，只喊一次。

    不喊的话升级是**静默降级**：原先靠这一项配归档目录的机器会无声地掉到用户缓存
    目录，那里是空的，于是候选池扫出 0 个源 —— 表现是「搜不到书」，而不是任何一条
    错误信息，排查起来要一路挖到这里。
    """
    global _LEGACY_CACHE_ROOT_WARNED
    with _LOCK:
        if _LEGACY_CACHE_ROOT_WARNED:
            return
        _LEGACY_CACHE_ROOT_WARNED = True
    stale = read_secret_value(cate2="cache", cate3="path", cate4="root")
    if stale is not None and str(stale).strip():
        logger.warning(
            "funsecret 的 funread/cache/path/root 已不再被读取（它是机器本地路径，"
            "不该放在跨机器共享的密钥库里），当前回落到 {}。"
            "要指定归档目录请写 config.toml 的 [cache] root。密钥库里那份是 {}",
            DEFAULT_CACHE_ROOT,
            stale,
        )


def resolve_database_url() -> str:
    """库地址：override > config.toml ``[database] url`` > funsecret > 本地 SQLite。

    funsecret 没配会兜到 :data:`DEFAULT_DATABASE_URL`，而不是让调用方失败。密钥库
    **坏掉**则原样抛 —— 默默兜底会藏住"这个进程到底在写哪个库"。
    """
    return str(
        resolve_setting(
            "database_url",
            candidates=(("database", "url"), (None, "database_url")),
            secret=("cache", "source", "db_url"),
            fallback=_fallback_to_default,
        )
    )


def resolve_cache_root() -> str:
    """源文件缓存目录：override > config.toml ``[cache] root`` > 用户缓存目录。

    兜底这一层是必须的。管线任务以前直接读 funsecret 且没有兜底，于是任何没配密钥库
    的进程（API 服务、新 clone、CI）都用不了。

    **这一项故意不走 funsecret**，是四个设置项里唯一的例外。密钥库是跨机器共享的，
    而这是一条机器本地路径：一台机器写进去，另一台就读到别人的路径。2026-10-15 两台
    机器同时栽在这上面 —— 密钥库里存着 funread-dat 还是独立仓库时的旧路径，Linux 那台
    因此扫出 0 个源（目录不存在，静默兜底成空的候选池），macOS 那台更直接，``/home``
    在 macOS 上是 autofs 挂载点，建目录当场 ``OSError: [Errno 45]``，搜索接口 500。

    判据是「这个值换台机器还成不成立」：库地址、口令换台机器照样对，归档目录不是。
    """
    return str(
        resolve_setting(
            "cache_root",
            candidates=(("cache", "root"), (None, "cache_root")),
            fallback=_fallback_cache_root,
        )
    )


def is_sqlite_url(database_url: str) -> bool:
    return database_url.startswith("sqlite")


def sqlite_path(database_url: str) -> Path | None:
    """Extract the filesystem path from a ``sqlite:///...`` URL, or ``None``."""
    if not is_sqlite_url(database_url) or ":memory:" in database_url:
        return None
    _, _, path = database_url.partition("sqlite:///")
    return Path(path) if path else None
