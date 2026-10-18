# test · 测试目录

量化引擎的全部测试脚本。**运行方式：任意 cwd 均可**（各测试文件头部已用 `Path(__file__).resolve().parent.parent` 自动把仓库根加入 sys.path）。2026-10-18 修正：本目录已在仓库根（coding/ 中间层于 10-16 撤销），下方命令按根目录口径。

## 文件清单

| 文件 | 类型 | 覆盖对象 | 行数 |
|---|---|---|---|
| `test_quant_engine_audit.py` | unittest（mock 隔离，推荐 CI 化） | BacktestEngine / DataCenter / SourceManager 审计 | 110 |
| `test_etf_holdings.py` | unittest（fixture 隔离，不打网络） | 天天基金持仓解析 / 季度 diff / 阈值过滤 / 监控流程 / 推送文案（17 项） | ~330 |
| `test_datacenter.py` | 脚本式（打真实接口） | DataCenter 实时行情聚合 | 19 |
| `test_eastmoney.py` | 脚本式（打真实接口） | EastMoneySource 全市场列表 | 15 |

> `test_etf_holdings.py` 的 fixture 保存在 `test/fixtures/jjcc_510300_{2025,2026}.txt`（2026-10-18 真实接口响应，天天基金 F10 持仓）。接口改版时该测试先红——发现解析漂移比生产里发现错数据便宜。

## 运行

```bash
# unittest 套件（不依赖外网，可重复跑）
cd test
../coding/.venv/bin/python -m unittest test_quant_engine_audit -v
../coding/.venv/bin/python test_etf_holdings.py

# 脚本式测试（会真实请求东财接口，注意 burst 封禁风险，见 prd/02 §3.2）
cd test
../coding/.venv/bin/python test_datacenter.py
../coding/.venv/bin/python test_eastmoney.py
```

> 2026-10-18 实跑：`test_etf_holdings.py` 17/17 通过 + `test_quant_engine_audit.py` 3/3 回归通过。注意 `test` 目录名与 Python 标准库 `test` 包冲突，`python3 -m unittest test.test_etf_holdings` 形式会失败，直接跑文件路径即可。

## 约定

- 新测试一律放本目录，命名 `test_*.py`
- 打真实接口的脚本式测试要在文件头注释标注"会请求外网"
- 项目当前无 pytest 依赖（`requirements.txt` 未含）， unittest 走标准库即可；如引入 pytest，在本目录加 `pytest.ini` 并更新 `quant_engine/requirements.txt`
- `__pycache__` 由 Python 自动生成，已在部署脚本排除列表中
