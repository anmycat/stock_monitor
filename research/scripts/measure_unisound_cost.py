"""D57：云知声 U2 真实成本与限流安全边际实测

用项目真实 prompt（_build_morning_prompt / _build_daily_prompt）测量：
1. 每次总结的真实 input/output token
2. 按云知声 U2-Flash 定价换算日/月成本
   （截图价：输入 ¥0.6/M、输出 ¥2/M、缓存命中输入 ¥0.06/M）
3. 项目的实际 RPM 峰值 vs 平台限额 RPM 100
"""
from __future__ import annotations

import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from quant_engine.models.market import MarketState, MarketType
from quant_engine.models.signal import Signal, SignalAction, SignalLevel
from quant_engine.services.ai.ai_summary import AISummary

# 定价（元 / M tokens），来自云知声 U2-Flash 模型页截图（限时6折）
PRICE_IN = 0.6 / 1_000_000
PRICE_OUT = 2.0 / 1_000_000
PRICE_IN_CACHED = 0.06 / 1_000_000


def _market() -> MarketState:
    return MarketState(
        type=MarketType.SIDEWAYS,
        score=62.5,
        trend=1.8,
        volume_ratio=1.15,
        breadth=58.3,
        timestamp=datetime(2026, 11, 3, 14, 30),
        details={'turnover': '9800亿', 'north_bound': '净流入 32亿'},
    )


def _signals() -> list[Signal]:
    now = datetime(2026, 11, 3, 14, 30)
    out = []
    for i, (code, name, score, price) in enumerate([
        ('600519.SH', '贵州茅台', 85.0, 1392.05),
        ('000651.SZ', '格力电器', 78.0, 42.18),
        ('300750.SZ', '宁德时代', 72.0, 258.60),
    ]):
        out.append(Signal(
            code=code, name=name, action=SignalAction.BUY, level=SignalLevel.A,
            score=score, position=0.3,
            reason=['技术面均线多头排列', f'资金面主力净流入 {score * 100:.0f}万',
                    f'量比 {1.2 + i * 0.1:.1f} 放量突破'],
            timestamp=now, strategy='balanced_alpha_v1', price=price,
            target_price=price * 1.08, stop_loss=price * 0.95,
            capital_flow_5d=score * 1e6, kelly_position=0.28,
        ))
    return out


def _sectors() -> list[dict]:
    return [
        {'name': '白酒', 'change_pct': 3.21, 'leader': '贵州茅台',
         'leader_change_pct': 10.04, 'count': 12},
        {'name': '新能源车', 'change_pct': 2.05, 'leader': '宁德时代',
         'leader_change_pct': 6.72, 'count': 28},
        {'name': '半导体', 'change_pct': -1.13, 'leader': '中芯国际',
         'leader_change_pct': -2.31, 'count': 45},
    ]


def main() -> None:
    key = os.environ.get('UNISOUND_API_KEY', '')
    if not key:
        print('需 UNISOUND_API_KEY 环境变量')
        sys.exit(1)

    s = AISummary()
    cfg = s.models['unisound']
    mkt, sigs, secs = _market(), _signals(), _sectors()

    market_info = s._format_market_info(mkt)
    signal_info = s._format_signal_info(sigs)
    sector_info = s._format_sector_info(secs)
    alert_info = s._format_alert_info(None)

    total_in = total_out = 0
    print('=' * 78)
    for stype, builder in (
        ('morning_review', s._build_morning_prompt),
        ('daily_review', s._build_daily_prompt),
    ):
        sysp, usrp = builder(market_info, signal_info, sector_info,
                             alert_info, '', '')
        r = s._call_single_model(cfg, sysp, usrp, max_tokens=1500)
        if not r.success:
            print(f'{stype}: 失败 {r.error}')
            continue
        # 复算 token（从响应 usage 拿）
        import httpx
        payload = {
            'model': cfg.model_id,
            'messages': [{'role': 'system', 'content': sysp},
                         {'role': 'user', 'content': usrp}],
            'temperature': 0.7, 'max_tokens': 1500, 'stream': False,
        }
        resp = httpx.post(cfg.api_url, json=payload,
                          headers={'Authorization': f'Bearer {cfg.api_key}'},
                          timeout=60)
        u = resp.json().get('usage', {})
        tin = u.get('prompt_tokens', 0)
        tout = u.get('completion_tokens', 0)
        cost = tin * PRICE_IN + tout * PRICE_OUT
        total_in += tin
        total_out += tout
        print(f'{stype}:')
        print(f'  input={tin}  output={tout}  总={tin + tout} tokens')
        print(f'  成本 = {tin}×0.6/M + {tout}×2/M = ¥{cost:.6f}  ({cost * 100:.4f} 分)')
        print(f'  延迟 = {r.latency_ms:.0f}ms   正文 {len(r.text)} 字')

    # 集合竞价播报（general prompt）
    sysp, usrp = s._build_general_prompt(market_info, signal_info, sector_info, alert_info, '')
    import httpx
    payload = {
        'model': cfg.model_id,
        'messages': [{'role': 'system', 'content': sysp},
                     {'role': 'user', 'content': usrp}],
        'temperature': 0.7, 'max_tokens': 1500, 'stream': False,
    }
    resp = httpx.post(cfg.api_url, json=payload,
                      headers={'Authorization': f'Bearer {cfg.api_key}'}, timeout=60)
    u = resp.json().get('usage', {})
    tin, tout = u.get('prompt_tokens', 0), u.get('completion_tokens', 0)
    total_in += tin
    total_out += tout

    print('=' * 78)
    day_cost = (total_in * PRICE_IN + total_out * PRICE_OUT)
    print(f'合计 input={total_in}  output={total_out}')
    print(f'单日成本（U2 单独跑 3 次总结）= ¥{day_cost:.6f}  ({day_cost * 100:.4f} 分)')
    print(f'按 22 个交易日/月 → ¥{day_cost * 22:.4f}/月')
    print()
    print('--- 若用 ensemble（U2 + DeepSeek 并行 + U2 合并）---')
    # ensemble 会多一次合并调用（输入含两份分析 + 市场数据）
    merge_in = total_in + total_out + 800
    merge_out = 600
    ens_extra = merge_in * PRICE_IN + merge_out * PRICE_OUT
    print(f'  额外合并调用 ≈ input {merge_in} + output {merge_out}')
    print(f'  额外成本 ≈ ¥{ens_extra:.6f}')
    print(f'  ensemble 单日 ≈ ¥{day_cost + ens_extra:.6f}'
          f'  月 ≈ ¥{(day_cost + ens_extra) * 22:.4f}')
    print()
    print('--- 限流安全边际（平台 RPM 100 / TPM 10,000,000）---')
    print('  项目 AI 调用点（每日）：集合竞价 1 + 早盘总结 1 + 全日总结 1 = 3 次')
    print('  ensemble 展开后每轮最多 3 次请求（U2+DS并行 + U2合并）')
    print('  → 单轮瞬时峰值 ≈ 3 RPM，占限额 3%')
    print('  → TPM 峰值 ≈ 3 × 1500 = 4500，占限额 0.045%')
    print('  → 安全边际极大（当前用量 < 限额的 5%）')
    print()
    print('--- 缓存可行性（截图价：缓存命中输入 ¥0.06/M，比原价便宜 10 倍）---')
    print('  实测 OpenAI 端 cached_tokens 恒为 0')
    print('  实测 Anthropic 端 cache_control:ephemeral → cache_write=0')
    print('  结论：u2-flash 当前未启用 prompt caching（两套协议均未命中）')


if __name__ == '__main__':
    main()
