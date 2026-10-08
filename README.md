# 阅读

[源仓库](http://www.yckceo1.com/)

[规则说明](https://alanskycn.gitee.io/teachme/)

[app端](https://github.com/gedoor/legado)

[服务端](https://github.com/hectorqin/reader)

`funread` 是一个用于管理和处理 Legado 阅读 APP 书源与 RSS 源的 Python 工具库。

## 安装

```bash
uv add funread
```

视频列表服务需要可选的 web 依赖：

```bash
uv add 'funread[web]'
```

## 视频列表服务

服务脚本的参数顺序为“操作 环境”。开发环境直接运行当前工作区源码：

```bash
scripts/setup.sh run dev
scripts/setup.sh start dev
scripts/setup.sh status dev
scripts/setup.sh stop dev
scripts/setup.sh restart dev
```

`run` 在前台运行；`start` 在后台运行，日志和 PID 文件分别位于
`.run/dev/web.log` 和 `.run/dev/web.pid`。默认监听 `8080` 端口，可通过
`FUNREAD_WEB_PORT_DEV`（或通用的 `FUNREAD_WEB_PORT`）设置，服务地址为
`http://127.0.0.1:<端口>/videos`。

生产环境只接受已经安装的 non-editable 正式包，脚本会在启动前校验该条件：

```bash
scripts/setup.sh run prod
scripts/setup.sh start prod
scripts/setup.sh status prod
scripts/setup.sh stop prod
scripts/setup.sh restart prod
```

生产环境使用独立的 `.run/prod/` 状态目录和 `FUNREAD_WEB_PORT_PROD` 端口配置。

## 最小示例

```python
from funread.legado.manage.source.storage import compute_url_md5

print(compute_url_md5("https://example.com/source.json"))
```

---

## 关于 farfarfun

[farfarfun](https://github.com/farfarfun) 是一个专注于实用工具库的开源组织，
涵盖云存储、数据处理、AI、多媒体与开发工具链等方向。

- 🏠 组织主页：<https://github.com/farfarfun>
- 📦 PyPI：<https://pypi.org/user/niuliangtao/>
- 📧 联系：farfarfun@qq.com

本项目基于 [MIT](LICENSE) 协议开源。
