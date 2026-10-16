# UX · UI/UED 交互目录

前端页面与交互设计资产。**当前内容为独立设计原型，尚未接入后端**（2026-10-16 全库实证：`quant_engine` Python 代码对 `static/`、`index.html`、`FileResponse`、`StaticFiles` 零引用，部署脚本也无 nginx/static 配置）。

## 文件清单

| 文件 | 大小 | 版本 | 说明 |
|---|---|---|---|
| `static/index.html` | 48KB | 2026-06-22 | Apple 风格 Web 界面（V5 改版同期产物），单文件无外部依赖 |

## 接入后端的方法（将来要做时）

后端用 FastAPI 托管本目录，挂载代码加在 `quant_engine/main.py`：

```python
from fastapi.staticfiles import StaticFiles
from pathlib import Path

# 路径不依赖 cwd：quant_engine/ 的上一级即 coding/
STATIC_DIR = Path(__file__).resolve().parent.parent / "UX" / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
```

再在 `api/routes.py` 加根路由返回 `FileResponse(STATIC_DIR / "index.html")`，即可通过 `http://<server>:8000/` 访问。

## 约定

- 页面、样式、交互原型、设计说明一律放本目录，与后端代码（quant_engine/）分离
- 单文件 HTML 是当前形态；如拆分为 css/js 资源，放 `UX/static/` 下并保持上述挂载路径有效
- 界面改版走本目录，改完在 `prd/07` 的 Web/前端章节记录版本与差异
