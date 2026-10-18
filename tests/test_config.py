"""配置解析四层链条：显式传参 > config.toml > funsecret > 兜底。

`conftest.py` 的 autouse fixture 已经 override 了 `database_url`/`cache_root`，
所以要测下面几层的用例必须先 `clear_overrides()`。
"""

import funsecret
import pytest

import funread.base.config as config


@pytest.fixture
def no_overrides():
    """卸掉 conftest 的隔离，让用例能看到 config.toml / funsecret / 兜底这几层。"""
    config.clear_overrides()
    yield


def _write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# 第 1 层：显式传参
# ---------------------------------------------------------------------------


def test_override_wins_over_config_file(tmp_path, monkeypatch):
    _write(tmp_path / "shared-config.toml", '[database]\nurl = "sqlite:///from-file.db"\n')
    monkeypatch.setattr(funsecret, "read_secret", lambda **_kwargs: "sqlite:///from-secret.db")

    with config.override(database_url="sqlite:///from-override.db"):
        assert config.resolve_database_url() == "sqlite:///from-override.db"


def test_override_restores_previous_value():
    """退出 with 之后要精确还原 —— 嵌套 override 不能把外层的值吃掉。"""
    with config.override(database_url="sqlite:///outer.db"):
        with config.override(database_url="sqlite:///inner.db"):
            assert config.resolve_database_url() == "sqlite:///inner.db"
        assert config.resolve_database_url() == "sqlite:///outer.db"


def test_override_none_removes_the_entry(no_overrides, tmp_path):
    _write(tmp_path / "shared-config.toml", '[database]\nurl = "sqlite:///from-file.db"\n')
    config.set_overrides(database_url="sqlite:///x.db")
    config.set_overrides(database_url=None)

    assert config.resolve_database_url() == "sqlite:///from-file.db"


# ---------------------------------------------------------------------------
# 第 2 层：config.toml
# ---------------------------------------------------------------------------


def test_shared_config_file_is_read(no_overrides, tmp_path, monkeypatch):
    _write(tmp_path / "shared-config.toml", '[database]\nurl = "sqlite:///from-file.db"\n')
    monkeypatch.setattr(funsecret, "read_secret", lambda **_kwargs: "sqlite:///from-secret.db")

    assert config.resolve_database_url() == "sqlite:///from-file.db"


def test_registered_file_beats_shared_file(no_overrides, tmp_path):
    _write(tmp_path / "shared-config.toml", '[database]\nurl = "sqlite:///shared.db"\n')
    service = _write(tmp_path / "api" / "config.toml", '[database]\nurl = "sqlite:///service.db"\n')
    config.use_config_file(service)

    assert config.resolve_database_url() == "sqlite:///service.db"


def test_shared_file_fills_gaps_in_registered_file(no_overrides, tmp_path):
    """服务文件只写了自己的 host/port 时，库地址还得从共享根文件里取到。"""
    _write(tmp_path / "shared-config.toml", '[database]\nurl = "sqlite:///shared.db"\n')
    service = _write(tmp_path / "api" / "config.toml", '[server]\nhost = "0.0.0.0"\nport = 1\n')
    config.use_config_file(service)

    assert config.resolve_database_url() == "sqlite:///shared.db"
    assert config.config_value(("server", "host")) == "0.0.0.0"


def test_flat_key_is_accepted(no_overrides, tmp_path):
    """`.env` 这种没有段的格式靠顶层扁平键，TOML 里也允许这么写。"""
    _write(tmp_path / "shared-config.toml", 'database_url = "sqlite:///flat.db"\n')

    assert config.resolve_database_url() == "sqlite:///flat.db"


def test_env_file_keys_drop_the_funread_prefix(no_overrides, tmp_path):
    service = _write(tmp_path / "custom.env", "FUNREAD_DATABASE_URL=sqlite:///from-env-file.db\n")
    config.use_config_file(service)

    assert config.resolve_database_url() == "sqlite:///from-env-file.db"


def test_missing_config_file_is_not_an_error(no_overrides, tmp_path, monkeypatch):
    """新装的机器上共享根文件本来就没有，不该因此失败。"""
    monkeypatch.setattr(funsecret, "read_secret", lambda **_kwargs: "sqlite:///from-secret.db")

    assert not (tmp_path / "shared-config.toml").exists()
    assert config.resolve_database_url() == "sqlite:///from-secret.db"


def test_broken_config_file_raises(no_overrides, tmp_path):
    """写坏了的 config.toml 必须响。当成空文件会让整台机器静默跑回兜底配置。"""
    _write(tmp_path / "shared-config.toml", "[database\nurl = oops\n")

    with pytest.raises(RuntimeError, match="cannot parse config file"):
        config.resolve_database_url()


def test_config_file_is_reread_after_it_changes(no_overrides, tmp_path):
    """带缓存，但文件变了要能看见 —— 否则 `start` 之前改的配置不生效。"""
    path = _write(tmp_path / "shared-config.toml", '[database]\nurl = "sqlite:///first.db"\n')
    assert config.resolve_database_url() == "sqlite:///first.db"

    _write(path, '[database]\nurl = "sqlite:///second.db"\n')
    config.reset_config_files()  # 清缓存

    assert config.resolve_database_url() == "sqlite:///second.db"


# ---------------------------------------------------------------------------
# 第 3 层：funsecret
# ---------------------------------------------------------------------------


def test_database_url_uses_secret(no_overrides, monkeypatch):
    monkeypatch.setattr(funsecret, "read_secret", lambda **_kwargs: "sqlite:///secret.db")

    assert config.resolve_database_url() == "sqlite:///secret.db"


def test_cache_root_ignores_the_secret_store(no_overrides, monkeypatch, tmp_path):
    """cache_root 是四项里唯一不走 funsecret 的。

    密钥库跨机器共享，而这是机器本地路径：存进去就意味着另一台机器读到别人的路径。
    真出过事 —— 密钥库里那条旧路径让一台 Linux 扫出 0 个源、一台 macOS 直接
    `OSError: [Errno 45]`。这条红了说明 `secret=` 被加回了 `resolve_cache_root`。
    """
    cache_root = tmp_path / "hub"
    monkeypatch.setattr(funsecret, "read_secret", lambda **_kwargs: "/tmp/secret-hub")
    monkeypatch.setattr(config, "DEFAULT_CACHE_ROOT", cache_root)
    monkeypatch.setattr(config, "_LEGACY_CACHE_ROOT_WARNED", False)

    assert config.resolve_cache_root() == str(cache_root)


def test_a_leftover_cache_root_secret_is_announced(no_overrides, monkeypatch, tmp_path):
    """密钥库里还压着旧值时必须喊一声，否则升级是静默降级成空候选池。"""
    warnings: list[str] = []
    monkeypatch.setattr(funsecret, "read_secret", lambda **_kwargs: "/tmp/secret-hub")
    monkeypatch.setattr(config, "DEFAULT_CACHE_ROOT", tmp_path / "hub")
    monkeypatch.setattr(config, "_LEGACY_CACHE_ROOT_WARNED", False)
    monkeypatch.setattr(
        config.logger, "warning", lambda message, *args: warnings.append(message.format(*args))
    )

    config.resolve_cache_root()
    config.resolve_cache_root()  # 只喊一次，不要每次解析都刷屏

    assert len(warnings) == 1
    assert "/tmp/secret-hub" in warnings[0]
    assert "[cache] root" in warnings[0]


def test_a_configured_cache_root_never_reads_the_secret_store(no_overrides, monkeypatch, tmp_path):
    """配了 `[cache] root` 的机器一次密钥库都不该查。

    检查旧值挂在兜底那条路上，不是解析链上 —— 否则就等于把刚拆掉的那一层又以
    「只读来警告」的形式加了回去，每次解析都要打一次密钥库。
    """

    def explode(**_kwargs):
        raise AssertionError("配置已命中，不该再读密钥库")

    monkeypatch.setattr(funsecret, "read_secret", explode)
    monkeypatch.setattr(config, "_LEGACY_CACHE_ROOT_WARNED", False)
    _write(tmp_path / "shared-config.toml", f'[cache]\nroot = "{tmp_path / "hubs"}"\n')

    assert config.resolve_cache_root() == str(tmp_path / "hubs")


def test_no_warning_when_the_secret_store_is_clean(no_overrides, monkeypatch, tmp_path):
    """没留旧值就别吓人。"""
    warnings: list[str] = []
    monkeypatch.setattr(funsecret, "read_secret", lambda **_kwargs: None)
    monkeypatch.setattr(config, "DEFAULT_CACHE_ROOT", tmp_path / "hub")
    monkeypatch.setattr(config, "_LEGACY_CACHE_ROOT_WARNED", False)
    monkeypatch.setattr(
        config.logger, "warning", lambda message, *args: warnings.append(message.format(*args))
    )

    config.resolve_cache_root()

    assert warnings == []


def test_database_url_propagates_secret_system_failure(no_overrides, monkeypatch):
    """secret store 坏掉跟没配是两回事：前者必须响，不能默默跑到本地 SQLite。"""
    monkeypatch.setattr(
        funsecret, "read_secret", lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("broken"))
    )

    with pytest.raises(RuntimeError, match="broken"):
        config.resolve_database_url()


# ---------------------------------------------------------------------------
# 第 4 层：兜底
# ---------------------------------------------------------------------------


def test_database_url_falls_back_to_local_sqlite(no_overrides, monkeypatch, tmp_path):
    database_path = tmp_path / "cache" / "funread.db"
    monkeypatch.setattr(
        funsecret, "read_secret", lambda **_kwargs: (_ for _ in ()).throw(KeyError())
    )
    monkeypatch.setattr(config, "DEFAULT_DATABASE_PATH", database_path)
    monkeypatch.setattr(config, "DEFAULT_DATABASE_URL", f"sqlite:///{database_path}")

    assert config.resolve_database_url() == f"sqlite:///{database_path}"
    assert database_path.parent.is_dir()


def test_cache_root_falls_back_without_secret_store(no_overrides, monkeypatch, tmp_path):
    """没配 funsecret 时必须有兜底 —— 以前这里直接抛，API 进程起不来。"""
    cache_root = tmp_path / "cache" / "hub"
    monkeypatch.setattr(
        funsecret, "read_secret", lambda **_kwargs: (_ for _ in ()).throw(KeyError("db_url"))
    )
    monkeypatch.setattr(config, "DEFAULT_CACHE_ROOT", cache_root)

    assert config.resolve_cache_root() == str(cache_root)
    assert cache_root.is_dir()


# ---------------------------------------------------------------------------
# 环境变量层已删除
# ---------------------------------------------------------------------------


#  两个变量分开测，因为参照物不同：database_url 掉到 funsecret，而 cache_root
#  已经没有 funsecret 那一层了（见 test_cache_root_ignores_the_secret_store），
#  它的参照物只能是兜底值。合在一个参数化里就得让断言含糊成「反正不等于环境变量」，
#  那样把「环境变量没参与」和「解析链整个坏掉」混成同一个绿。
def test_the_database_url_env_var_is_ignored(no_overrides, monkeypatch):
    """钉住环境变量层的删除。

    这条红了说明有人把 `os.environ.get` 加回了解析链。那不只是"多了一层"：两个仓库
    所有测试的生产库隔离都建立在"环境变量不参与解析"之上，加回来等于把隔离交还给
    一个谁都能设的全局变量。
    """
    monkeypatch.setenv("FUNREAD_DATABASE_URL", "sqlite:///should-be-ignored")
    monkeypatch.setattr(funsecret, "read_secret", lambda **_kwargs: "sqlite:///from-secret.db")

    assert config.resolve_database_url() == "sqlite:///from-secret.db"


def test_the_cache_root_env_var_is_ignored(no_overrides, monkeypatch, tmp_path):
    """同上，但参照物是兜底值。

    `DEFAULT_CACHE_ROOT` 必须一起 monkeypatch：兜底会**建目录**，不挡住的话这条
    用例会在跑测试的人的真实家目录下留下 ~/.cache/farfarfun/funread/hub。
    """
    fallback = tmp_path / "fallback-hub"
    monkeypatch.setenv("FUNREAD_CACHE_ROOT", str(tmp_path / "from-env"))
    monkeypatch.setattr(config, "DEFAULT_CACHE_ROOT", fallback)
    monkeypatch.setattr(config, "_LEGACY_CACHE_ROOT_WARNED", True)

    assert config.resolve_cache_root() == str(fallback)


def test_ignored_env_vars_reports_what_is_still_set(monkeypatch):
    monkeypatch.setenv("FUNREAD_DATABASE_URL", "sqlite:///x.db")
    monkeypatch.delenv("FUNREAD_CACHE_ROOT", raising=False)

    assert config.ignored_env_vars(config.DEPRECATED_ENV_VARS) == ["FUNREAD_DATABASE_URL"]


def test_ignored_env_vars_ignores_blank_values(monkeypatch):
    """`export FUNREAD_CACHE_ROOT=` 不算"设了"，不该吵。"""
    monkeypatch.setenv("FUNREAD_CACHE_ROOT", "   ")

    assert config.ignored_env_vars(config.DEPRECATED_ENV_VARS) == []


def test_describe_ignored_env_names_the_replacement_key(monkeypatch):
    """提示里必须说清该改到哪 —— 只说"已忽略"等于把人丢在原地。"""
    monkeypatch.setenv("FUNREAD_DATABASE_URL", "sqlite:///x.db")
    monkeypatch.delenv("FUNREAD_CACHE_ROOT", raising=False)

    (message,) = config.describe_ignored_env(config.DEPRECATED_ENV_VARS)

    assert "FUNREAD_DATABASE_URL" in message
    assert "[database] url" in message


# ---------------------------------------------------------------------------
# 杂项
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (True, True),
        (False, False),
        ("1", True),
        ("true", True),
        ("YES", True),
        ("on", True),
        ("0", False),
        ("false", False),
        ("", False),
        ("nonsense", False),
        (1, True),
        (0, False),
    ],
)
def test_as_bool(raw, expected):
    assert config.as_bool(raw) is expected


def test_config_revision_changes_on_every_mutation():
    """funread-api 的 service 容器按它做缓存键，所以必须真的动。"""
    before = config.config_revision()
    config.set_overrides(database_url="sqlite:///x.db")
    after = config.config_revision()
    config.clear_overrides()

    assert after != before
    assert config.config_revision() != after


def test_a_remote_database_is_announced_once_with_the_password_redacted(
    no_overrides, monkeypatch, tmp_path
):
    """这台机器的默认库是生产 PostgreSQL；忘了隔离的脚本至少要在第一次写入之前看到一行。"""
    warnings: list[str] = []
    monkeypatch.setattr(config, "_REMOTE_DATABASE_WARNED", False)
    monkeypatch.setattr(
        config.logger, "warning", lambda message, *args: warnings.append(message.format(*args))
    )
    _write(
        tmp_path / "shared-config.toml",
        '[database]\nurl = "postgresql+psycopg2://funread:S3cret@db.example.com:5432/funread"\n',
    )

    config.resolve_database_url()
    config.resolve_database_url()

    assert len(warnings) == 1
    assert "db.example.com" in warnings[0] and "S3cret" not in warnings[0]
    assert "set_overrides" in warnings[0]


def test_a_local_sqlite_database_is_not_announced(no_overrides, monkeypatch, tmp_path):
    warnings: list[str] = []
    monkeypatch.setattr(config, "_REMOTE_DATABASE_WARNED", False)
    monkeypatch.setattr(
        config.logger, "warning", lambda message, *args: warnings.append(message.format(*args))
    )
    _write(tmp_path / "shared-config.toml", f'[database]\nurl = "sqlite:///{tmp_path}/x.db"\n')
    config.resolve_database_url()
    assert warnings == []
