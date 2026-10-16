# test · 测试目录

量化引擎的全部测试脚本。**运行方式：在 `coding/` 的上一级或本目录外执行，确保 `coding/` 在 sys.path**（各测试文件头部已用 `Path(__file__).resolve().parent.parent` 自动处理，从任意 cwd 运行均可）。

## 文件清单

| 文件 | 类型 | 覆盖对象 | 行数 |
|---|---|---|---|
| `test_quant_engine_audit.py` | unittest（mock 隔离，推荐 CI 化） | BacktestEngine / DataCenter / SourceManager 审计 | 110 |
| `test_datacenter.py` | 脚本式（打真实接口） | DataCenter 实时行情聚合 | 19 |
| `test_eastmoney.py` | 脚本式（打真实接口） | EastMoneySource 全市场列表 | 15 |

## 运行

```bash
# unittest 套件（不依赖外网，可重复跑）
cd coding/test
python3 -m unittest test_quant_engine_audit -v

# 脚本式测试（会真实请求东财接口，注意 burst 封禁风险，见 prd/02 §3.2）
cd coding/test
python3 test_datacenter.py
python3 test_eastmoney.py
```

## 约定

- 新测试一律放本目录，命名 `test_*.py`
- 打真实接口的脚本式测试要在文件头注释标注"会请求外网"
- 项目当前无 pytest 依赖（`requirements.txt` 未含）， unittest 走标准库即可；如引入 pytest，在本目录加 `pytest.ini` 并更新 `quant_engine/requirements.txt`
- `__pycache__` 由 Python 自动生成，已在部署脚本排除列表中
