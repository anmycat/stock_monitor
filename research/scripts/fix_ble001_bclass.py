"""P5-b：B 类 BLE001 59 处逐处分级（prd/15 §3.2 阶段六）。

决策（基于 dump_ble001_context.py 全量上下文人工审读）：
- 收窄 4 处：错误类型明确可预期的纯逻辑/解析处（yaml_loader 文件解析/
  registry 字典构造/yaml_executor 指标比较/signal_generator pandas 运算）
- noqa 55 处：单项跳过/降级默认/任务循环兜底/缓存 IO 兜底/外部调用兜底——
  宽捕获是设计语义（收窄会漏未预期错误导致整扫描崩溃），逐处附理由标签

理由标签（与 prd/15 §3.2 阶段六口径一致）：
  单项跳过  循环内单项失败不影响整体
  降级默认  失败走默认值/透传（原行为）
  任务兜底  调度/编排循环不死
  缓存IO    缓存读写失败降级
  外部调用  外部服务失败兜底
用法：python research/scripts/fix_ble001_bclass.py [--dry-run]
"""
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]

# (相对路径, except 行号): noqa 理由
NOQA_MAP = {
    # --- 任务/编排循环兜底 ---
    ("quant_engine/worker.py", 91): "任务兜底",
    ("quant_engine/worker.py", 101): "任务兜底",
    ("quant_engine/worker.py", 132): "任务兜底",
    ("quant_engine/worker.py", 158): "任务兜底",
    ("quant_engine/worker.py", 183): "任务兜底",
    ("quant_engine/worker.py", 214): "任务兜底",
    ("quant_engine/worker.py", 240): "任务兜底",
    ("quant_engine/worker.py", 367): "任务兜底",
    ("quant_engine/beat.py", 59): "任务兜底",
    ("quant_engine/main.py", 139): "任务兜底",
    ("quant_engine/services/ai/agent_router.py", 112): "任务兜底",
    # --- 缓存 IO 兜底 ---
    ("quant_engine/services/portfolio.py", 63): "缓存IO",
    ("quant_engine/services/portfolio.py", 72): "缓存IO",
    ("quant_engine/backtest/post_evaluation.py", 36): "缓存IO",
    ("quant_engine/backtest/post_evaluation.py", 124): "缓存IO",
    ("quant_engine/backtest/post_evaluation.py", 161): "缓存IO",
    ("quant_engine/backtest/post_evaluation.py", 182): "缓存IO",
    ("quant_engine/services/monitors/etf_holdings_monitor.py", 351): "缓存IO",
    ("quant_engine/services/monitors/etf_holdings_monitor.py", 360): "缓存IO",
    ("quant_engine/services/code_guard.py", 107): "缓存IO",
    ("quant_engine/services/code_guard.py", 137): "缓存IO",
    ("quant_engine/services/code_guard.py", 472): "缓存IO",
    ("quant_engine/services/code_guard.py", 481): "缓存IO",
    ("quant_engine/services/code_guard.py", 533): "缓存IO",
    # --- 外部调用兜底 ---
    ("quant_engine/services/monitors/etf_holdings_monitor.py", 59): "外部调用",
    ("quant_engine/services/monitors/etf_monitor.py", 74): "外部调用",
    ("quant_engine/services/monitors/burst_monitor.py", 62): "外部调用",
    ("quant_engine/services/monitors/burst_monitor.py", 89): "外部调用",
    ("quant_engine/services/monitors/sector_monitor.py", 50): "外部调用",
    ("quant_engine/services/signals/candidate_context.py", 57): "外部调用",
    ("quant_engine/services/signals/signal_generator.py", 73): "外部调用",
    ("quant_engine/services/signals/signal_generator.py", 177): "外部调用",
    ("quant_engine/services/market/market_alert.py", 77): "外部调用",
    ("quant_engine/services/market/market_alert.py", 120): "外部调用",
    ("quant_engine/services/market/market_alert.py", 162): "外部调用",
    ("quant_engine/services/market/market_alert.py", 187): "外部调用",
    ("quant_engine/services/market/market_alert.py", 216): "外部调用",
    ("quant_engine/services/code_guard.py", 186): "外部调用",
    ("quant_engine/services/code_guard.py", 420): "外部调用",
    ("quant_engine/strategies/funnel.py", 65): "外部调用",
    ("quant_engine/strategies/funnel.py", 75): "外部调用",
    ("quant_engine/strategies/yaml_loader.py", 32): "外部调用",
    # --- 单项跳过（循环内）---
    ("quant_engine/backtest/engine.py", 41): "单项跳过",
    ("quant_engine/strategies/funnel.py", 102): "单项跳过",
    ("quant_engine/strategies/funnel.py", 149): "单项跳过",
    ("quant_engine/strategies/funnel.py", 210): "单项跳过",
    ("quant_engine/services/signals/signal_generator.py", 62): "单项跳过",
    ("quant_engine/services/monitors/etf_holdings_monitor.py", 118): "单项跳过",
    # --- 降级默认/透传 ---
    ("quant_engine/services/market/market_analyzer.py", 52): "降级默认",
    ("quant_engine/services/market/market_analyzer.py", 94): "降级默认",
    ("quant_engine/services/signals/signal_generator.py", 50): "降级默认",
    ("quant_engine/strategies/gates.py", 90): "降级默认",
    ("quant_engine/strategies/factor_scorer.py", 43): "降级默认",
    ("quant_engine/services/risk/position_sizer.py", 115): "降级默认",
    ("quant_engine/strategies/dispatcher.py", 214): "降级默认",
}

# 收窄 4 处：(相对路径, 行号, 原文本, 新文本)
NARROW = [
    ("quant_engine/strategies/yaml_loader.py", 43,
     "        except Exception as e:",
     "        except (OSError, yaml.YAMLError) as e:"),
    ("quant_engine/strategies/registry.py", 112,
     "            except Exception as e:",
     "            except (KeyError, TypeError, ValueError) as e:"),
    ("quant_engine/strategies/yaml_executor.py", 156,
     "        except Exception as e:",
     "        except (TypeError, ValueError) as e:"),
    ("quant_engine/services/signals/signal_generator.py", 147,
     "        except Exception:",
     "        except (KeyError, TypeError, ValueError):"),
]

# 收窄需要的 import 补充（若文件尚未导入）
IMPORT_FIX = {
    "quant_engine/strategies/yaml_loader.py": (
        "import yaml", "import yaml"),
}


def main():
    dry = "--dry-run" in sys.argv
    # 1) 收窄
    for rel, ln, old, new in NARROW:
        p = ROOT / rel
        lines = p.read_text(encoding="utf-8").splitlines()
        idx = ln - 1
        if lines[idx].strip() != old.strip():
            print(f"SKIP-NARROW {rel}:{ln} 行内容不匹配: {lines[idx]!r}")
            continue
        lines[idx] = new
        if not dry:
            p.write_text("\n".join(lines) + "\n", encoding="utf-8")
        print(f"{'DRY' if dry else 'NARROW'} {rel}:{ln} -> {new.strip()}")

    # 2) noqa 批量（按文件分组，行号倒序应用防漂移）
    by_file = {}
    for (rel, ln), reason in NOQA_MAP.items():
        by_file.setdefault(rel, []).append((ln, reason))
    n = 0
    for rel, items in by_file.items():
        p = ROOT / rel
        lines = p.read_text(encoding="utf-8").splitlines()
        for ln, reason in sorted(items, reverse=True):
            idx = ln - 1
            cur = lines[idx]
            if "except Exception" not in cur:
                print(f"SKIP-NOQA {rel}:{ln} 非 except Exception 行: {cur!r}")
                continue
            if "noqa" in cur:
                print(f"SKIP-NOQA {rel}:{ln} 已有 noqa")
                continue
            lines[idx] = f"{cur}  # noqa: BLE001 {reason}"
            n += 1
        if not dry:
            p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"\n{'would annotate' if dry else 'annotated'}: {n} 处 noqa（映射 {len(NOQA_MAP)} 处）")


if __name__ == "__main__":
    main()
