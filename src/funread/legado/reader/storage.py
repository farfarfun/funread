"""阅读端的持久化：账号、源偏好、书架、阅读进度、章节缓存。

和 `manage/source/storage.py` 同一套路子（SQLAlchemy declarative + `create_all`
+ 手写迁移，不引 Alembic），但表是独立的一组，engine / session 工厂复用那边的缓存
—— 同一个库开两个连接池没有意义。

## 数据按人隔离

书架、进度（以及订阅源那两张表）都带 `user_id`，查询一律进 SQL 的 WHERE 而不是
在应用层过滤 —— 漏一处就是跨用户数据泄露。章节正文缓存 `reader_chapter_cache`
**不带** `user_id`：它是正文缓存而不是个人数据，按人隔离只会让同一章正文存 N 份。

`user_id = 0`（`LOCAL_USER_ID`）是「还没有任何账号」时的隐式单人身份，保证新克隆
与 CI 不配账号也能直接用。第一个账号注册成功时会把 user 0 的数据认领过去
（`claim_local_data`），之后 user 0 不再可达。
"""

import hashlib
import hmac
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from farlog import getLogger
from sqlalchemy import (
    Boolean,
    DateTime,
    Integer,
    String,
    Text,
    delete,
    func,
    inspect,
    select,
    text,
    update,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

from ..manage.source.storage import _get_engine, _get_session_factory, utcnow

logger = getLogger("funread")

#: 还没有任何注册账号时使用的隐式身份。见模块 docstring。
LOCAL_USER_ID = 0

#: 用户名规则：字母数字加下划线短横点，3-32 位。限死是为了让它能安全地出现在
#: 日志、URL 和错误文案里，不必再逐处转义。
USERNAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{2,31}$")

MIN_PASSWORD_LENGTH = 8

#: scrypt 参数。n=2**14 在本机约 60ms —— 对登录够快，对离线爆破够慢。
#: 存进哈希串里，所以以后调大不会让旧口令失效。
_SCRYPT_N = 2**14
_SCRYPT_R = 8
_SCRYPT_P = 1
_SCRYPT_SALT_BYTES = 16
_SCRYPT_KEY_BYTES = 32


class ReaderBase(DeclarativeBase):
    """阅读端表的 declarative base。

    和采集侧的 `Base` 分开：`init_reader_db()` 不该顺手把采集侧的表也建出来，
    反过来也一样。
    """


class ReaderUser(ReaderBase):
    """一个阅读端账号。

    和 B 端 `/admin` 的单口令完全分开 —— 管理端不该和读者账号共用凭据。
    口令只存 scrypt 哈希，明文不落库也不进日志。
    """

    __tablename__ = "reader_user"

    user_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    username: Mapped[str] = mapped_column(String(32), nullable=False, unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    disabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow, nullable=False
    )


class ReaderSourcePref(ReaderBase):
    """一个源在阅读端的可用性与排序权重。

    `is_complete` / `needs_js` 是**静态**扫描结果（源 JSON 里的八个核心字段齐不齐、
    有没有 JS 规则），一次算好存下来，不必每次搜索都重新解析 13k 个文件。
    `fail_count` / `last_ok_at` 是**动态**的实跑结果 —— 语料里 `status==2` 的源
    绝大多数其实已经死了，真正能用哪些只能靠跑出来。
    """

    __tablename__ = "reader_source_prefs"

    source_type: Mapped[str] = mapped_column(String(32), primary_key=True)
    url_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, index=True)
    weight: Mapped[int] = mapped_column(Integer, nullable=False, default=0, index=True)
    is_complete: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    needs_js: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    #: 这个源有没有发现页规则（`exploreUrl` + 列表规则）。扫描时算好存下来 ——
    #: 不存的话「列出能浏览分类的源」就要现场读 5,606 个 JSON 文件。
    has_explore: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)
    fail_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_ok_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_error: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow, nullable=False
    )


class ReaderShelfBook(ReaderBase):
    """书架上的一本书。

    主键是 `(user_id, book_key)`，`book_key` 是书名+作者的 md5 而不是 (源, bookUrl)：
    同一本书在不同源下的 URL 完全不同，换源时如果主键跟着源走，书架上就会冒出两条
    同名记录、进度也各记各的。源相关的字段（`source_type`/`url_id`/`book_url`）是
    「当前读的是哪个源」，换源时原地改写。

    `user_id` 进主键而不是只做个索引列：两个人各自把同一本书加进书架是完全正常的，
    `book_key` 单独做主键会让第二个人加不进去。
    """

    __tablename__ = "reader_shelf"

    user_id: Mapped[int] = mapped_column(Integer, primary_key=True, default=LOCAL_USER_ID)
    book_key: Mapped[str] = mapped_column(String(32), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    author: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    cover_url: Mapped[str] = mapped_column(String(1024), nullable=False, default="")
    intro: Mapped[str] = mapped_column(Text, nullable=False, default="")
    source_type: Mapped[str] = mapped_column(String(32), nullable=False, default="book")
    url_id: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    book_url: Mapped[str] = mapped_column(String(1024), nullable=False, default="")
    toc_url: Mapped[str] = mapped_column(String(1024), nullable=False, default="")
    last_chapter: Mapped[str] = mapped_column(String(512), nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow, nullable=False
    )


class ReaderProgress(ReaderBase):
    """阅读进度。一人一本书一条，跟着 `book_key` 走，换源不丢。

    `char_offset` 存的是正文里的字符偏移而不是滚动像素 —— 换了字号/字体/设备之后
    像素值毫无意义，字符偏移还能换算回大致位置。
    """

    __tablename__ = "reader_progress"

    user_id: Mapped[int] = mapped_column(Integer, primary_key=True, default=LOCAL_USER_ID)
    book_key: Mapped[str] = mapped_column(String(32), primary_key=True)
    chapter_index: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    chapter_url: Mapped[str] = mapped_column(String(1024), nullable=False, default="")
    chapter_name: Mapped[str] = mapped_column(String(512), nullable=False, default="")
    char_offset: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow, nullable=False
    )


class ReaderChapterCache(ReaderBase):
    """已抓取的章节正文。既是缓存也是离线下载的落点。

    **故意不带 `user_id`**：这是正文缓存，不是个人数据。按人隔离只会让同一章正文
    存 N 份，而正文本身没有任何隐私含义 —— 它就是公网上那一页。代价是下架时不能
    无条件清缓存，见 `remove_shelf_book`。
    """

    __tablename__ = "reader_chapter_cache"

    book_key: Mapped[str] = mapped_column(String(32), primary_key=True)
    chapter_index: Mapped[int] = mapped_column(Integer, primary_key=True)
    chapter_url: Mapped[str] = mapped_column(String(1024), nullable=False, default="")
    title: Mapped[str] = mapped_column(String(512), nullable=False, default="")
    content: Mapped[str] = mapped_column(Text, nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)


class ReaderRssSubscription(ReaderBase):
    """一条订阅。两种来源共用一张表，由 `kind` 区分。

    `kind="legado"` → 用归档里的 Legado RSS 源（`url_id` 指过去）；
    `kind="feed"`  → 用户自己贴的标准 feed 地址（`feed_url`）。

    两条腿共用同一张表、同一组 API，是因为对前端来说它们就是「我的订阅」里的
    一行，没有任何交互差别。差别只在抓取时怎么解析，那是 `rss_service` 的事。

    `(user_id, sub_id)` 复合主键：`sub_id` 由 `(kind, 来源标识)` 算出来，所以两个
    人订同一个源会得到同一个 `sub_id`，必须带上 `user_id` 才不会撞。
    """

    __tablename__ = "reader_rss_subscription"

    user_id: Mapped[int] = mapped_column(Integer, primary_key=True, default=LOCAL_USER_ID)
    sub_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    kind: Mapped[str] = mapped_column(String(16), nullable=False, default="feed")
    #: `kind="legado"` 时有效，指向归档里的源。
    url_id: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    #: `kind="feed"` 时有效，用户给的 feed 地址。
    feed_url: Mapped[str] = mapped_column(String(1024), nullable=False, default="")
    title: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    icon: Mapped[str] = mapped_column(String(1024), nullable=False, default="")
    group: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    last_fetched_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_error: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow, nullable=False
    )


class ReaderRssArticleState(ReaderBase):
    """一篇文章的已读 / 收藏状态。

    **只存状态，不存正文。** 订阅文章时效性强，缓存正文的收益很低而占用很高 ——
    一个活跃订阅一周就是几千篇。标题和链接存一份是为了「收藏」列表能脱离原始
    列表单独渲染（文章翻过几页之后原列表就拿不到了）。

    `article_key` 是 `md5(link)`：feed 里的 guid 五花八门（有的根本没有），
    链接是唯一一个所有来源都有、且稳定的标识。
    """

    __tablename__ = "reader_rss_article_state"

    user_id: Mapped[int] = mapped_column(Integer, primary_key=True, default=LOCAL_USER_ID)
    sub_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    article_key: Mapped[str] = mapped_column(String(32), primary_key=True)
    read: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)
    favorited: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)
    title: Mapped[str] = mapped_column(String(512), nullable=False, default="")
    link: Mapped[str] = mapped_column(String(1024), nullable=False, default="")
    pub_date: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    image: Mapped[str] = mapped_column(String(1024), nullable=False, default="")
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow, nullable=False
    )


_INITIALIZED_DATABASES: set = set()


def compute_book_key(name: str, author: str = "") -> str:
    """书名 + 作者的稳定标识。

    作者可能缺失（搜索结果里很常见），所以归一化成 `书名\\n作者` 再取 md5 —— 空作者
    照样得到一个稳定的 key，而不是和另一本同名书撞上。
    """
    normalized = f"{(name or '').strip()}\n{(author or '').strip()}"
    if not normalized.strip():
        raise ValueError("name is required")
    return hashlib.md5(normalized.encode("utf-8")).hexdigest()


#: 带 `user_id` 的表，以及它们在补上这一列之前的主键。迁移要按这张表重建。
_USER_SCOPED_TABLES = {
    "reader_shelf": "book_key",
    "reader_progress": "book_key",
}

#: 后来补的普通列（不进主键），`ALTER TABLE ADD COLUMN` 就够。
#: 格式：表名 → [(列名, DDL 片段)]
_ADDED_COLUMNS = {
    "reader_source_prefs": [("has_explore", "BOOLEAN NOT NULL DEFAULT 0")],
}


def _migrate_added_columns(engine) -> None:
    """给已有表补上后来加的普通列。

    和 `_migrate_user_scope` 分开：那个要重建表（主键变了），这个只是加列。
    加列是幂等的 —— 先看 `inspect` 里有没有，有就跳过。
    """
    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())
    for table, columns in _ADDED_COLUMNS.items():
        if table not in existing_tables:
            continue
        present = {column["name"] for column in inspector.get_columns(table)}
        for name, ddl in columns:
            if name in present:
                continue
            with engine.begin() as connection:
                connection.execute(text(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}"))
            logger.info(f"{table} 已补上 {name} 列")


def _migrate_user_scope(engine) -> None:
    """给 M2 时建的 `reader_shelf` / `reader_progress` 补上 `user_id` 主键列。

    不能用 `ALTER TABLE ADD COLUMN` 了事 —— `user_id` 要进**主键**，而 SQLite 改不了
    已有表的主键。所以走标准的重建三步：旧表改名 → 按新形状建表 → 带着
    `LOCAL_USER_ID` 把数据搬过去。搬完核对行数，不一致就抛，宁可迁移失败也不要
    悄悄丢几本书。

    存量行归到 `user_id = 0`，第一个注册的账号会通过 `claim_local_data` 认领。
    """
    inspector = inspect(engine)
    existing = set(inspector.get_table_names())
    stale = {
        name
        for name in _USER_SCOPED_TABLES
        if name in existing
        and "user_id" not in {column["name"] for column in inspector.get_columns(name)}
    }
    if not stale:
        return

    for name in sorted(stale):
        legacy = f"{name}__pre_user"
        with engine.begin() as connection:
            before = connection.execute(text(f"SELECT COUNT(*) FROM {name}")).scalar_one()
            #  上一次迁移中途挂掉留下的残渣，先清掉再重来
            connection.execute(text(f"DROP TABLE IF EXISTS {legacy}"))
            connection.execute(text(f"ALTER TABLE {name} RENAME TO {legacy}"))

        ReaderBase.metadata.tables[name].create(engine)

        with engine.begin() as connection:
            columns = [
                column["name"]
                for column in inspect(engine).get_columns(legacy)
                if column["name"] != "user_id"
            ]
            joined = ", ".join(columns)
            connection.execute(
                text(
                    f"INSERT INTO {name} (user_id, {joined}) "
                    f"SELECT {LOCAL_USER_ID}, {joined} FROM {legacy}"
                )
            )
            after = connection.execute(text(f"SELECT COUNT(*) FROM {name}")).scalar_one()
            if after != before:
                raise RuntimeError(
                    f"{name} 迁移后行数不符：迁移前 {before}，迁移后 {after}。"
                    f"旧数据仍在 {legacy}，没有删除。"
                )
            connection.execute(text(f"DROP TABLE {legacy}"))
        logger.info(f"{name} 已补上 user_id 列，{before} 行归到 user {LOCAL_USER_ID}")


def init_reader_db(database_url: Optional[str] = None) -> None:
    engine = _get_engine(database_url)
    key = str(engine.url)
    if key in _INITIALIZED_DATABASES:
        return
    #  先迁移再 create_all：create_all 只会跳过已存在的表，不会去改它的形状，
    #  所以旧表必须在这之前重建好。
    _migrate_user_scope(engine)
    _migrate_added_columns(engine)
    ReaderBase.metadata.create_all(engine)
    _INITIALIZED_DATABASES.add(key)


# ------------------------------------------------------------------ 口令哈希


def hash_password(password: str) -> str:
    """`scrypt$n$r$p$salt_hex$key_hex`。

    用 stdlib 的 `hashlib.scrypt`，不引 passlib/bcrypt —— funread 的依赖面要维持
    现状，而 scrypt 本身就是为抗硬件爆破设计的。参数编进字符串，以后调大不会让
    已有口令失效。
    """
    if not password:
        raise ValueError("password is required")
    salt = os.urandom(_SCRYPT_SALT_BYTES)
    key = hashlib.scrypt(
        password.encode("utf-8"),
        salt=salt,
        n=_SCRYPT_N,
        r=_SCRYPT_R,
        p=_SCRYPT_P,
        dklen=_SCRYPT_KEY_BYTES,
        maxmem=64 * 1024 * 1024,
    )
    return f"scrypt${_SCRYPT_N}${_SCRYPT_R}${_SCRYPT_P}${salt.hex()}${key.hex()}"


def verify_password(password: str, stored: str) -> bool:
    """永不抛：哈希串格式坏了就是验证失败，不是 500。"""
    try:
        scheme, n, r, p, salt_hex, key_hex = (stored or "").split("$")
        if scheme != "scrypt":
            return False
        candidate = hashlib.scrypt(
            (password or "").encode("utf-8"),
            salt=bytes.fromhex(salt_hex),
            n=int(n),
            r=int(r),
            p=int(p),
            dklen=len(bytes.fromhex(key_hex)),
            maxmem=64 * 1024 * 1024,
        )
    except (ValueError, TypeError, MemoryError):
        return False
    #  compare_digest，不是 ==：普通比较会按时间泄露哈希
    return hmac.compare_digest(candidate.hex(), key_hex)


# ------------------------------------------------------------------ 账号


def count_users(database_url: Optional[str] = None) -> int:
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        return int(session.execute(select(func.count()).select_from(ReaderUser)).scalar_one())


def get_user_by_name(username: str, database_url: Optional[str] = None) -> Optional[ReaderUser]:
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        stmt = select(ReaderUser).where(ReaderUser.username == (username or "").strip())
        return session.execute(stmt).scalars().first()


def get_user(user_id: int, database_url: Optional[str] = None) -> Optional[ReaderUser]:
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        return session.get(ReaderUser, int(user_id))


def claim_local_data(user_id: int, database_url: Optional[str] = None) -> Dict[str, int]:
    """把 `user_id = 0` 的存量数据认领给某个账号。

    只在第一个账号注册时调用：那之前的书架与进度是「还没有账号时」攒下来的，
    理应归第一个人。第二个账号注册时已经没有 user 0 的行了，所以这个函数返回全零。
    """
    moved: Dict[str, int] = {}
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        for model in (ReaderShelfBook, ReaderProgress):
            result = session.execute(
                update(model).where(model.user_id == LOCAL_USER_ID).values(user_id=int(user_id))
            )
            moved[model.__tablename__] = int(result.rowcount or 0)
        session.commit()
    return moved


def create_user(
    username: str,
    password: str,
    database_url: Optional[str] = None,
) -> ReaderUser:
    """建账号。用户名已存在抛 `ValueError`，调用方把它翻成 409。

    第一个账号会顺带认领 user 0 的存量数据 —— 它本来就是同一个人在没有账号时读的。
    """
    username = (username or "").strip()
    if not USERNAME_PATTERN.match(username):
        raise ValueError("用户名需为 3-32 位字母、数字、下划线、短横线或点，且以字母数字开头")
    if len(password or "") < MIN_PASSWORD_LENGTH:
        raise ValueError(f"口令至少 {MIN_PASSWORD_LENGTH} 位")

    session_factory = get_session_factory(database_url)
    is_first = count_users(database_url) == 0
    with session_factory() as session:
        if session.execute(
            select(ReaderUser.user_id).where(ReaderUser.username == username)
        ).first():
            raise ValueError("用户名已被占用")
        row = ReaderUser(username=username, password_hash=hash_password(password))
        session.add(row)
        session.commit()
        session.refresh(row)
        created = row

    if is_first:
        moved = claim_local_data(created.user_id, database_url=database_url)
        if any(moved.values()):
            logger.info(f"首个账号 {username} 认领了无主数据：{moved}")
    return created


def authenticate(
    username: str,
    password: str,
    database_url: Optional[str] = None,
) -> Optional[ReaderUser]:
    """校验口令。用户不存在与口令不对返回同一个 `None` —— 不泄露用户名是否存在。"""
    user = get_user_by_name(username, database_url=database_url)
    if user is None:
        #  走一遍同等开销的哈希，避免「用户不存在」比「口令错」快得多，
        #  那本身就是一个可枚举用户名的信道
        verify_password(password or "", hash_password("timing-equaliser"))
        return None
    if user.disabled:
        return None
    if not verify_password(password or "", user.password_hash):
        return None
    return user


def get_session_factory(database_url: Optional[str] = None) -> sessionmaker:
    init_reader_db(database_url=database_url)
    return _get_session_factory(database_url=database_url)


# ------------------------------------------------------------------ 源偏好


def upsert_source_prefs(
    records: List[Dict[str, Any]],
    database_url: Optional[str] = None,
) -> int:
    """批量写入源偏好。已存在的按字段覆盖，没给的字段保持原值。

    注册表扫描一次会写几千条，所以走一个事务、一次性 `merge`。
    """
    if not records:
        return 0
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        existing = {
            (row.source_type, row.url_id): row
            for row in session.execute(select(ReaderSourcePref)).scalars().all()
        }
        for payload in records:
            key = (str(payload["source_type"]), int(payload["url_id"]))
            row = existing.get(key)
            if row is None:
                row = ReaderSourcePref(source_type=key[0], url_id=key[1])
                session.add(row)
                existing[key] = row
            for field, value in payload.items():
                if field in ("source_type", "url_id"):
                    continue
                if hasattr(row, field):
                    setattr(row, field, value)
        session.commit()
    return len(records)


def list_source_prefs(
    source_type: str = "book",
    enabled_only: bool = True,
    limit: Optional[int] = None,
    explore_only: bool = False,
    database_url: Optional[str] = None,
) -> List[ReaderSourcePref]:
    """按「真跑通过的优先、失败少优先、权重高优先」列出源。

    排序顺序就是选源策略的全部 —— 聚合搜索只取前 N 个，排错了等于没过滤。
    第一项是 `last_ok_at IS NULL` 升序：**实跑成功过**的源排在从没试过的前面。
    采集侧的 `status==2` 只说明某次 GET 过站点首页，拿它当「能读」的依据会得到
    个位数百分比的命中率；真正可信的信号只有自己跑出来的那一次成功。
    """
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        stmt = select(ReaderSourcePref).where(ReaderSourcePref.source_type == source_type)
        if enabled_only:
            stmt = stmt.where(ReaderSourcePref.enabled.is_(True))
        if explore_only:
            stmt = stmt.where(ReaderSourcePref.has_explore.is_(True))
        stmt = stmt.order_by(
            ReaderSourcePref.last_ok_at.is_(None).asc(),
            ReaderSourcePref.fail_count.asc(),
            ReaderSourcePref.weight.desc(),
            ReaderSourcePref.url_id.asc(),
        )
        if limit is not None:
            stmt = stmt.limit(int(limit))
        return list(session.execute(stmt).scalars().all())


def record_source_result(
    source_type: str,
    url_id: int,
    ok: bool,
    error: str = "",
    disable_after: Optional[int] = None,
    database_url: Optional[str] = None,
) -> None:
    """记一次实跑结果。成功清零失败计数，失败累加。

    `disable_after` 给的是「连续失败多少次就停用」。默认不停用：站点临时抽风很常见，
    自动停用太激进会把能用的源一点点耗光。真正该立刻停用的是
    `JsRequiredError` 那种结构性不支持 —— 调用方显式传 `disable_after=1`。
    """
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        row = session.get(ReaderSourcePref, (source_type, int(url_id)))
        if row is None:
            row = ReaderSourcePref(source_type=source_type, url_id=int(url_id))
            session.add(row)
        if ok:
            row.fail_count = 0
            row.last_ok_at = utcnow()
            row.last_error = None
        else:
            row.fail_count = int(row.fail_count or 0) + 1
            row.last_error = (error or "")[:1024] or None
            if disable_after is not None and row.fail_count >= int(disable_after):
                row.enabled = False
        session.commit()


# ------------------------------------------------------------------ 书架


def list_shelf(
    user_id: int = LOCAL_USER_ID,
    database_url: Optional[str] = None,
) -> List[ReaderShelfBook]:
    """某个人的书架，最近读过的排前面。"""
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        stmt = (
            select(ReaderShelfBook)
            .where(ReaderShelfBook.user_id == int(user_id))
            .order_by(ReaderShelfBook.updated_at.desc())
        )
        return list(session.execute(stmt).scalars().all())


def get_shelf_book(
    book_key: str,
    user_id: int = LOCAL_USER_ID,
    database_url: Optional[str] = None,
) -> Optional[ReaderShelfBook]:
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        return session.get(ReaderShelfBook, (int(user_id), book_key))


def upsert_shelf_book(
    payload: Dict[str, Any],
    user_id: int = LOCAL_USER_ID,
    database_url: Optional[str] = None,
) -> str:
    """加书入架 / 更新书架条目，返回 `book_key`。

    调用方可以不给 `book_key`，由书名+作者算出来 —— 前端拿到的搜索结果里本来就
    只有这两样。`user_id` 由调用方给，**不从 payload 里取** —— payload 来自 HTTP
    请求体，让它能指定 user_id 等于让任何人往别人书架里塞书。
    """
    book_key = payload.get("book_key") or compute_book_key(
        str(payload.get("name") or ""), str(payload.get("author") or "")
    )
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        row = session.get(ReaderShelfBook, (int(user_id), book_key))
        if row is None:
            row = ReaderShelfBook(user_id=int(user_id), book_key=book_key)
            session.add(row)
        for field, value in payload.items():
            if field in ("book_key", "user_id"):
                continue
            if hasattr(row, field) and value is not None:
                setattr(row, field, value)
        row.updated_at = utcnow()
        session.commit()
    return book_key


def remove_shelf_book(
    book_key: str,
    user_id: int = LOCAL_USER_ID,
    database_url: Optional[str] = None,
) -> bool:
    """下架。连带清掉**这个人的**进度。

    章节缓存只在没有别人也把这本书放在架上时才清 —— 缓存是全局共享的，无条件清掉
    会顺手废掉另一个人已经下载好的章节。
    """
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        row = session.get(ReaderShelfBook, (int(user_id), book_key))
        if row is None:
            return False
        session.delete(row)
        session.execute(
            delete(ReaderProgress).where(
                ReaderProgress.user_id == int(user_id),
                ReaderProgress.book_key == book_key,
            )
        )
        session.flush()
        others = session.execute(
            select(func.count())
            .select_from(ReaderShelfBook)
            .where(ReaderShelfBook.book_key == book_key)
        ).scalar_one()
        if not others:
            session.execute(
                delete(ReaderChapterCache).where(ReaderChapterCache.book_key == book_key)
            )
        session.commit()
        return True


# ------------------------------------------------------------------ 进度


def get_progress(
    book_key: str,
    user_id: int = LOCAL_USER_ID,
    database_url: Optional[str] = None,
) -> Optional[ReaderProgress]:
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        return session.get(ReaderProgress, (int(user_id), book_key))


def save_progress(
    book_key: str,
    chapter_index: int,
    chapter_url: str = "",
    chapter_name: str = "",
    char_offset: int = 0,
    user_id: int = LOCAL_USER_ID,
    database_url: Optional[str] = None,
) -> None:
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        row = session.get(ReaderProgress, (int(user_id), book_key))
        if row is None:
            row = ReaderProgress(user_id=int(user_id), book_key=book_key)
            session.add(row)
        row.chapter_index = int(chapter_index)
        row.chapter_url = chapter_url or ""
        row.chapter_name = chapter_name or ""
        row.char_offset = max(0, int(char_offset))
        row.updated_at = utcnow()
        #  书架上的 updated_at 跟着动，书架排序才会把在读的书顶上去
        book = session.get(ReaderShelfBook, (int(user_id), book_key))
        if book is not None:
            book.updated_at = row.updated_at
        session.commit()


# ------------------------------------------------------------------ 章节缓存


def get_cached_chapter(
    book_key: str,
    chapter_index: int,
    database_url: Optional[str] = None,
) -> Optional[ReaderChapterCache]:
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        return session.get(ReaderChapterCache, (book_key, int(chapter_index)))


def save_cached_chapter(
    book_key: str,
    chapter_index: int,
    chapter_url: str,
    title: str,
    content: str,
    database_url: Optional[str] = None,
) -> None:
    """写章节缓存。空正文不写 —— 把一次失败的抓取缓存下来等于永久坏掉这一章。"""
    if not (content or "").strip():
        return
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        row = session.get(ReaderChapterCache, (book_key, int(chapter_index)))
        if row is None:
            row = ReaderChapterCache(book_key=book_key, chapter_index=int(chapter_index))
            session.add(row)
        row.chapter_url = chapter_url or ""
        row.title = title or ""
        row.content = content
        session.commit()


def list_cached_chapter_indexes(
    book_key: str,
    database_url: Optional[str] = None,
) -> List[int]:
    """已缓存的章节序号。前端据此画「已下载」标记，不必逐章问。"""
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        stmt = (
            select(ReaderChapterCache.chapter_index)
            .where(ReaderChapterCache.book_key == book_key)
            .order_by(ReaderChapterCache.chapter_index)
        )
        return [int(value) for value in session.execute(stmt).scalars().all()]


def clear_chapter_cache(book_key: str, database_url: Optional[str] = None) -> int:
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        result = session.execute(
            delete(ReaderChapterCache).where(ReaderChapterCache.book_key == book_key)
        )
        session.commit()
        return int(result.rowcount or 0)


# ------------------------------------------------------------------ 订阅源


def compute_sub_id(kind: str, identifier: str) -> str:
    """订阅的稳定标识。

    由 `(kind, 来源标识)` 算出来而不是自增：同一个人重复订同一个源应当是幂等的，
    而自增主键会让它变成两条。两个人订同一个源会得到同一个 `sub_id`，所以表的
    主键必须是 `(user_id, sub_id)`。
    """
    normalized = f"{(kind or '').strip()}\n{(identifier or '').strip()}"
    if not (identifier or "").strip():
        raise ValueError("identifier is required")
    return hashlib.md5(normalized.encode("utf-8")).hexdigest()


def compute_article_key(link: str) -> str:
    """`md5(link)`。feed 的 guid 五花八门，链接是唯一普遍可用的稳定标识。"""
    normalized = (link or "").strip()
    if not normalized:
        raise ValueError("link is required")
    return hashlib.md5(normalized.encode("utf-8")).hexdigest()


def list_subscriptions(
    user_id: int = LOCAL_USER_ID,
    database_url: Optional[str] = None,
) -> List[ReaderRssSubscription]:
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        stmt = (
            select(ReaderRssSubscription)
            .where(ReaderRssSubscription.user_id == int(user_id))
            .order_by(ReaderRssSubscription.created_at.asc())
        )
        return list(session.execute(stmt).scalars().all())


def get_subscription(
    sub_id: str,
    user_id: int = LOCAL_USER_ID,
    database_url: Optional[str] = None,
) -> Optional[ReaderRssSubscription]:
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        return session.get(ReaderRssSubscription, (int(user_id), sub_id))


def upsert_subscription(
    payload: Dict[str, Any],
    user_id: int = LOCAL_USER_ID,
    database_url: Optional[str] = None,
) -> str:
    """订阅 / 更新订阅，返回 `sub_id`。重复订阅同一个源是幂等的。

    `user_id` 由调用方给，不从 payload 取 —— 同 `upsert_shelf_book` 的理由。
    """
    kind = str(payload.get("kind") or "feed")
    identifier = (
        str(payload.get("url_id") or "") if kind == "legado" else str(payload.get("feed_url") or "")
    )
    sub_id = payload.get("sub_id") or compute_sub_id(kind, identifier)
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        row = session.get(ReaderRssSubscription, (int(user_id), sub_id))
        if row is None:
            row = ReaderRssSubscription(user_id=int(user_id), sub_id=sub_id, kind=kind)
            session.add(row)
        for field_name, value in payload.items():
            if field_name in ("sub_id", "user_id"):
                continue
            if hasattr(row, field_name) and value is not None:
                setattr(row, field_name, value)
        row.updated_at = utcnow()
        session.commit()
    return sub_id


def remove_subscription(
    sub_id: str,
    user_id: int = LOCAL_USER_ID,
    database_url: Optional[str] = None,
) -> bool:
    """退订。连带清掉这个人在这个订阅下的全部文章状态。"""
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        row = session.get(ReaderRssSubscription, (int(user_id), sub_id))
        if row is None:
            return False
        session.delete(row)
        session.execute(
            delete(ReaderRssArticleState).where(
                ReaderRssArticleState.user_id == int(user_id),
                ReaderRssArticleState.sub_id == sub_id,
            )
        )
        session.commit()
        return True


def record_subscription_fetch(
    sub_id: str,
    user_id: int = LOCAL_USER_ID,
    error: str = "",
    database_url: Optional[str] = None,
) -> None:
    """记一次抓取结果。成功清掉上次的错误，失败留下原因给界面显示。"""
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        row = session.get(ReaderRssSubscription, (int(user_id), sub_id))
        if row is None:
            return
        row.last_fetched_at = utcnow()
        row.last_error = (error or "")[:1024] or None
        session.commit()


def get_article_states(
    sub_id: str,
    article_keys: List[str],
    user_id: int = LOCAL_USER_ID,
    database_url: Optional[str] = None,
) -> Dict[str, ReaderRssArticleState]:
    """批量取状态，按 `article_key` 索引。

    批量而不是逐条：一页文章二十条，逐条查会是二十次往返。
    """
    if not article_keys:
        return {}
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        stmt = select(ReaderRssArticleState).where(
            ReaderRssArticleState.user_id == int(user_id),
            ReaderRssArticleState.sub_id == sub_id,
            ReaderRssArticleState.article_key.in_(list(article_keys)),
        )
        return {row.article_key: row for row in session.execute(stmt).scalars().all()}


def set_article_state(
    sub_id: str,
    article_key: str,
    user_id: int = LOCAL_USER_ID,
    read: Optional[bool] = None,
    favorited: Optional[bool] = None,
    meta: Optional[Dict[str, str]] = None,
    database_url: Optional[str] = None,
) -> None:
    """标已读 / 收藏。`read` 与 `favorited` 给 `None` 表示不动那一项。

    `meta`（标题/链接/时间/配图）在建行时写一份，之后不覆盖 —— 收藏列表要能脱离
    原始列表单独渲染，而文章翻过几页之后原列表就拿不到这些字段了。
    """
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        row = session.get(ReaderRssArticleState, (int(user_id), sub_id, article_key))
        if row is None:
            row = ReaderRssArticleState(
                user_id=int(user_id), sub_id=sub_id, article_key=article_key
            )
            for name, value in (meta or {}).items():
                if hasattr(row, name) and value is not None:
                    setattr(row, name, value)
            session.add(row)
        if read is not None:
            row.read = bool(read)
        if favorited is not None:
            row.favorited = bool(favorited)
        row.updated_at = utcnow()
        session.commit()


def mark_all_read(
    sub_id: str,
    article_keys: List[str],
    user_id: int = LOCAL_USER_ID,
    metas: Optional[Dict[str, Dict[str, str]]] = None,
    database_url: Optional[str] = None,
) -> int:
    """把给定的一批文章全标已读，返回实际改动的条数。

    要调用方把 `article_keys` 传进来，而不是「这个订阅下的全部」：服务端并不
    知道这个订阅一共有哪些文章 —— 状态表里只有被交互过的那些。界面上「全部
    已读」的语义本来也是「当前列出来的这些」。
    """
    if not article_keys:
        return 0
    session_factory = get_session_factory(database_url)
    changed = 0
    with session_factory() as session:
        existing = {
            row.article_key: row
            for row in session.execute(
                select(ReaderRssArticleState).where(
                    ReaderRssArticleState.user_id == int(user_id),
                    ReaderRssArticleState.sub_id == sub_id,
                    ReaderRssArticleState.article_key.in_(list(article_keys)),
                )
            )
            .scalars()
            .all()
        }
        for key in article_keys:
            row = existing.get(key)
            if row is None:
                row = ReaderRssArticleState(
                    user_id=int(user_id), sub_id=sub_id, article_key=key, read=True
                )
                for name, value in ((metas or {}).get(key) or {}).items():
                    if hasattr(row, name) and value is not None:
                        setattr(row, name, value)
                session.add(row)
                changed += 1
            elif not row.read:
                row.read = True
                row.updated_at = utcnow()
                changed += 1
        session.commit()
    return changed


def count_read(
    sub_id: str,
    user_id: int = LOCAL_USER_ID,
    database_url: Optional[str] = None,
) -> int:
    """这个订阅下已读了多少篇。

    注意**没有**「未读数」—— 服务端不知道一个订阅总共有多少篇文章（不缓存列表），
    所以未读数只能由前端用「本页条数 - 本页已读数」算，算的是当前这一页。
    """
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        return int(
            session.execute(
                select(func.count())
                .select_from(ReaderRssArticleState)
                .where(
                    ReaderRssArticleState.user_id == int(user_id),
                    ReaderRssArticleState.sub_id == sub_id,
                    ReaderRssArticleState.read.is_(True),
                )
            ).scalar_one()
        )


def list_favorites(
    user_id: int = LOCAL_USER_ID,
    sub_id: Optional[str] = None,
    database_url: Optional[str] = None,
) -> List[ReaderRssArticleState]:
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        stmt = select(ReaderRssArticleState).where(
            ReaderRssArticleState.user_id == int(user_id),
            ReaderRssArticleState.favorited.is_(True),
        )
        if sub_id:
            stmt = stmt.where(ReaderRssArticleState.sub_id == sub_id)
        stmt = stmt.order_by(ReaderRssArticleState.updated_at.desc())
        return list(session.execute(stmt).scalars().all())


__all__ = [
    "LOCAL_USER_ID",
    "MIN_PASSWORD_LENGTH",
    "ReaderBase",
    "ReaderChapterCache",
    "ReaderProgress",
    "ReaderRssArticleState",
    "ReaderRssSubscription",
    "ReaderShelfBook",
    "ReaderSourcePref",
    "ReaderUser",
    "USERNAME_PATTERN",
    "authenticate",
    "claim_local_data",
    "clear_chapter_cache",
    "compute_article_key",
    "compute_book_key",
    "compute_sub_id",
    "count_read",
    "count_users",
    "create_user",
    "get_article_states",
    "get_cached_chapter",
    "get_progress",
    "get_session_factory",
    "get_shelf_book",
    "get_subscription",
    "get_user",
    "get_user_by_name",
    "hash_password",
    "init_reader_db",
    "list_cached_chapter_indexes",
    "list_favorites",
    "list_shelf",
    "list_source_prefs",
    "list_subscriptions",
    "mark_all_read",
    "record_source_result",
    "record_subscription_fetch",
    "remove_shelf_book",
    "remove_subscription",
    "save_cached_chapter",
    "save_progress",
    "set_article_state",
    "upsert_shelf_book",
    "upsert_source_prefs",
    "upsert_subscription",
    "verify_password",
]
