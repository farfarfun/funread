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
