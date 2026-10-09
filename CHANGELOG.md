# 更新日志

## 未发布

### 新增

- 无。

### 修复

- 修复 `fundrive`（仅要求 Python >= 3.12）条件依赖导致 Python 3.10/3.11 下
  `import funread.legado.manage` 整体失败的问题：`download/context.py`、
  `publish/entrance.py`、`publish/rss.py` 改为惰性导入 `fundrive`，缺失时仅在
  实际使用发布/上传功能（`SourceBuildContext`、`UpdateEntrance`、
  `UpdateRssTask`）时抛出明确的 `ImportError`，不再拖垮整个包的导入。
- 修复 `download/reporting/builder.py` 中订阅源链接硬编码指向不存在的
  `funread/legado/rss/rss-main.json`、导致 Legado 导入页链接全部失效的问题，
  改为指向实际产出的 `funread/legado/snapshot/lasted/funread.json`
  （根因排查见 farfarfun/funread-cache#743，该仓库已同步修正生成产物里的链接）。
- 收窄多处过宽的 `except Exception`：`utils/core.py` 的 `url_to_hostname`、
  `source/check/task.py` 与 `source/merge/task.py` 中 `read_secret` 相关的
  数据库地址读取、`_load_status_map`、`_read_file_status`/`_read_version`
  （及 merge 侧的 `_read_version_count`）、`source/sync/task.py` 的
  `_build_canonical_url_id_map`，均改为捕获实际可能抛出的具体异常类型并保留
  日志上下文，避免掩盖真实的系统故障。
- 简化 `download/core/processor.py` 中 `(IOError, Exception)` 这一冗余异常元组
  （`IOError` 是 `OSError` 的别名，本身就是 `Exception` 的子类，并无实际收窄
  效果）。

### 变更

- `fundrive` 依赖从无条件声明改为真正可选，移动到 `publish` extra：
  需要发布/上传功能的调用方请改用 `pip install funread[publish]`
  （仍要求 Python >= 3.12）。
- 将遗留的 `typing.Optional/List/Dict/Set/Tuple/Type` 写法统一迁移为 Python
  3.10+ 内置泛型语法（`X | None`、`list[X]`、`dict[K, V]` 等）。
- 为 `download/sources/factory.py`（模块、`register_source_type`、
  `supported_source_types`、`SourceStoreFactory.create`）与
  `download/sources/book.py` 的 `BookSourceFormat.run` 补充中文 docstring。
- `web/page/video_list.py` 的 `run()` 新增 `port` 参数，支持通过
  `FUNREAD_WEB_PORT` 环境变量覆盖监听端口；新增 `scripts/setup.sh` 管理该
  开发用 NiceGUI 服务的 start/stop/restart/run/status 生命周期。

### 废弃

- 无。

## 1.1.103

### 新增

- 无。

### 修复

- 修正 Python 3.10 兼容声明、日志版本下限和解析器失败行为。
- 移除视频列表调试输出。

### 变更

- 现代化公开类型标注并补充最小使用示例。

### 废弃

- 无。
