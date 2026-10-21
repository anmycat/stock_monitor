#!/usr/bin/env python3
"""PRD 内容审计 · 代码实证核查脚本（D29 第一轮）
对 prd/ 文档中的可验证声明逐项核对生产代码，输出 PASS/FAIL 表。
"""
import json
import os
import re
import sys

ROOT = "/Users/hr.wh/Documents/stock_monitor"
QE = os.path.join(ROOT, "quant_engine")

results = []


def check(name, actual, expected, note=""):
    ok = actual == expected
    results.append((name, actual, expected, "PASS" if ok else "FAIL", note))


def read(p):
    with open(p, encoding="utf-8") as f:
        return f.read()


# 1. 路由数（07 §7 称 36 HTTP + 1 WS + 1 根路由 = 38；12/14 称 40/42）
routes_py = read(os.path.join(QE, "api/routes.py"))
http_routes = len(re.findall(r"@router\.(get|post|put|delete|patch)\(", routes_py))
main_py = read(os.path.join(QE, "main.py"))
ws_routes = len(re.findall(r"@app\.websocket\(", main_py)) + len(
    re.findall(r"@router\.websocket\(", routes_py))
root_routes = len(re.findall(r'@app\.(get|post)\("/"\)', main_py))
check("routes.py HTTP 路由数", http_routes, None, "07 §7 称 36；12/14 称 40/42")
check("WebSocket 路由数", ws_routes, 1, "07 §7 称 /ws 1 个")
check("根路由 / 数", root_routes, None, "07 §7 称 1 个")

# 2. Python 文件数（00 §1.3 称 82 个文件；12 §1.1 称 81 个）
py_files = []
for dirpath, dirnames, filenames in os.walk(QE):
    dirnames[:] = [d for d in dirnames if d != "__pycache__"]
    py_files += [os.path.join(dirpath, f) for f in filenames if f.endswith(".py")]
check("quant_engine Python 文件数", len(py_files), None, "00 §1.3 称 82；12 §1.1 称 81")

# 3. main.py / routes.py 行数（13 §3.2 称 151 / 833）
check("main.py 行数", len(main_py.splitlines()), 151, "13 §3.2 称 151")
check("routes.py 行数", len(routes_py.splitlines()), 833, "13 §3.2 称 833")

# 4. beat 周期（00 §3.1/07 §3.2 称 sleep 120s；07 §13.1 称 300s 周期）
beat_py = read(os.path.join(QE, "beat.py"))
m = re.findall(r"sleep\s*\(\s*(\d+)\s*\)", beat_py)
check("beat.py sleep 值(秒)", m, None, "00/07 §3.2 称 120s；07 §13.1 称 300s 周期")
check("beat.py 含 300", "300" in beat_py, None, "性能调研称 300s 周期")

# 5. _PRIORITY 矩阵（02 §3.8 vs §4.2 矛盾表）
src_py = read(os.path.join(QE, "data/source.py"))
pri = re.search(r"_PRIORITY\s*=\s*\{(.+?)\}", src_py, re.S)
check("_PRIORITY 存在", pri is not None, True)
if pri:
    block = pri.group(1)
    for cap in ["realtime", "stock_list", "daily", "minute", "etf"]:
        mm = re.search(cap + r'"\s*:\s*\[([^\]]*)\]', block)
        check(f"_PRIORITY[{cap}]", mm.group(1).strip() if mm else None, None,
              "02 §3.8: realtime=[tencent,sina,eastmoney] stock_list=[sina,eastmoney] daily=[tencent,sina,eastmoney] minute=[tdx]（P0 后口径）")

# 6. stock_list 缓存 TTL（02 §4.2 称 86400s；03 §4.2 称 300s）
dc_py = read(os.path.join(QE, "data/repositories/market_data.py"))  # P1 重构路径（原 datacenter.py）
for key in ["stock_list", "all_realtime_quotes", "quote", "daily", "minute"]:
    mm = re.search(rf'"{key}"\s*:\s*(\d+)', dc_py)
    check(f"datacenter TTL {key}", mm.group(1) if mm else None, None,
          "02 §4.2: quote60/all30/daily3600/stock_list86400/minute300；03 §4.2: stock_list 300s")

# 7. signal_cooldown TTL（01 §5.2 称 4h；03 §4.2 称 86400s）
cm_hits = []
for dirpath, dirnames, filenames in os.walk(QE):
    dirnames[:] = [d for d in dirnames if d != "__pycache__"]
    for f in filenames:
        if f.endswith(".py"):
            t = read(os.path.join(dirpath, f))
            if "signal_cooldown" in t:
                for mm in re.finditer(r"signal_cooldown[^\n]*?(\d{3,5})", t):
                    cm_hits.append((f, mm.group(0)[:80]))
check("signal_cooldown TTL 出现处", cm_hits, None, "01 §5.2 称 4 小时；03 §4.2 称 86400s")

# 8. 八因子权重（04 §6：0.12/0.15/0.10/0.10/0.10/0.15/0.13/0.15）
fs_py = read(os.path.join(QE, "strategies/factor_scorer.py"))
weights = re.findall(r'"[^"]*"\s*:\s*(0\.\d+)', fs_py)
check("factor_scorer 权重值集合", sorted(set(weights)), None,
      "04 §6: 估值.12 动量.15 流动.10 稳定.10 量能.10 趋势.15 资金.13 技术.15")

# 9. Dispatcher 融合 70/30（00 §3.1/04 §6/05 §2.1）
disp_py = read(os.path.join(QE, "strategies/dispatcher.py"))
check("dispatcher 含 0.7/0.3", ("0.7" in disp_py and "0.3" in disp_py), True)
check("dispatcher 含 40/55 阈值", ("40" in disp_py and "55" in disp_py), True, "因子<40 拒 BUY <55 降级")

# 10. L1 反信号过滤阈值（01 §7.2：高位 0.92/连涨 8 天/假突破 0.93）
filters_py = read(os.path.join(QE, "strategies/filters.py"))
check("filters 高位阈值 0.92", "0.92" in filters_py, True, "01 §7.2")
check("filters 连涨 8", ("8" in filters_py), True)
check("filters 假突破 0.93", "0.93" in filters_py, True)

# 11. L2 门控阈值（01 §7.2：vol_ma5 ≥ vol_ma20×0.4；RSI≤88）
gates_py = read(os.path.join(QE, "strategies/gates.py"))
check("gates 含 0.4", "0.4" in gates_py, True, "资金门控 vol_ma5≥vol_ma20×0.4")
check("gates 含 88", "88" in gates_py, True, "触发门控 RSI≤88")

# 12. 策略文件（01 §7.1：趋势+低吸+动量+均值回归+防守；05 §5：3 YAML）
strat_dir = os.path.join(QE, "strategies")
yaml_files = sorted(f for f in os.listdir(strat_dir) if f.endswith(".yaml"))
check("YAML 策略文件", yaml_files, ["balanced_alpha_v1.yaml", "dip_reversal_v1.yaml", "trend_breakout_v1.yaml"])
check("内置策略文件", sorted(f for f in os.listdir(strat_dir) if f in ("trend.py", "dip.py", "defense.py", "momentum.py", "mean_reversion.py")),
      ["defense.py", "dip.py", "trend.py"], "01 §7.1 称含动量+均值回归（代码无对应文件则为过期声明）")

# 13. rules JSON 现状（v1.4 结构 + R12 状态 + Kelly 值 + R06 direction）
rules = json.load(open(os.path.join(ROOT, "research/rules/methodology_rules.json"), encoding="utf-8"))
check("rules JSON _meta.version", rules.get("_meta", {}).get("version"), "1.4", "文档称 v1.4 结构 + v1.5 裁定未写入")
tr = rules.get("trade_rules", {})
r06 = tr.get("R06_inverse_T", {})
check("R06 direction", r06.get("direction"), None, "v1.5 裁定应为 avoidance（JSON 未升版则为 buy）")
r12 = tr.get("R12_expected_gap", {}) or tr.get("R12", {})
check("R12 配置", {k: r12.get(k) for k in ("enabled", "default", "active", "params") if k in r12}, None,
      "v1.5 裁定默认开启（弱过滤 pre10>25%）")
kp = rules.get("kelly", {}).get("params", {})
check("Kelly R02", kp.get("R02") if isinstance(kp, dict) else None, None, "v1.5 应为 9.15；v1.4 为 13.13")
check("Kelly R01", kp.get("R01") if isinstance(kp, dict) else None, None, "v1.5 应为 2.46；v1.4 为 12.18")

# 14. R12 半年数字溯源（04/05/10 称 +0.83→+0.73；06 §7.4 称 +0.22→+0.17）
val6 = rules.get("_6m_validation", {})
ac6 = val6.get("adopted_config_6m_v2", {})
s = json.dumps(val6, ensure_ascii=False)
check("_6m_validation 含 0.83", "0.83" in s, None, "04/05/10 的 6M R12 单独数字来源")
check("_6m_validation 含 0.17", "0.17" in s, None, "06 §7.4 的 6M R12 单独数字来源")
check("adopted_config_6m_v2 键", list(ac6.keys()) if isinstance(ac6, dict) else str(ac6)[:60], None,
      "应含无过滤/仅R12/仅R13/组合四行——用于裁定 06 §7.4 与 04/05/10 的数字矛盾")

# 15. TDX market 映射（生产代码应为 深0沪1）
tdx_py = read(os.path.join(QE, "data/sources/tdx.py"))
check("tdx MARKET_TO_TDX 深0沪1", ("MARKET_TO_TDX" in tdx_py), True)
mm = re.search(r"MARKET_TO_TDX\s*=\s*\{([^}]*)\}", tdx_py)
check("tdx 映射内容", mm.group(1).strip() if mm else None, None, "应为 'SZ': 0, 'SH': 1")

# 16. 数据源文件清单（11 §5 缺 tdx/fund_holdings/news_sources/code_utils）
src_dir = os.path.join(QE, "data/sources")
check("data/sources 文件", sorted(f for f in os.listdir(src_dir) if f.endswith(".py")), None, "11 §5 只列了 eastmoney/tencent/sina")
check("utils/code_utils.py 存在", os.path.exists(os.path.join(QE, "utils/code_utils.py")), True, "11 §5 工具行未列")

# 17. is_trading_time 口径（02 §3.8：9:30-11:30/13:00-15:00 不含竞价）
cal_py = read(os.path.join(QE, "utils/calendar.py"))
check("calendar 9:30 口径", "9:30" in cal_py or "9, 30" in cal_py or "570" in cal_py, True)

# 输出
print(f"{'核查项':<44} {'实际值':<28} {'预期':<10} 结果  备注")
print("-" * 130)
fails = 0
for name, actual, expected, status, note in results:
    if status == "FAIL":
        fails += 1
    a = str(actual)[:26] if actual is not None else "-"
    e = str(expected) if expected is not None else "-"
    print(f"{name:<44} {a:<28} {e:<10} {status}  {note}")
print("-" * 130)
print(f"合计 {len(results)} 项，FAIL {fails} 项（expected=None 的为信息项，需人工判读）")
sys.exit(0)
