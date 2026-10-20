"""起爆资金监控模块 - 检测瞬间异动（基于main66.py逻辑）"""
import time
from datetime import datetime
from typing import List, Dict
from loguru import logger
from ..data.datacenter import DataCenter
from ..utils.calendar import is_trading_time
import pandas as pd


class BurstMonitor:
    """起爆资金监控 - 检测瞬间涨速和成交额异动"""

    def __init__(self, datacenter: DataCenter):
        self.datacenter = datacenter
        # 核心策略参数（参考main66.py）
        self.target_count = 300       # 监控成交额前300名的活跃股
        self.speed_threshold = 0.4    # 涨速阈值 (0.4%)
        self.amount_threshold = 8000000  # 瞬时成交额阈值 (800万)
        self.min_gain = 2.0           # 最低涨幅过滤
        self.max_gain = 7.5           # 最高涨幅过滤
        self.min_price = 2.0          # 最低价格过滤（排除低价股）
        # 历史数据（类似main66.py的快照机制）
        self.last_snapshot: Dict[str, Dict] = {}
        self.alerted: Dict[str, float] = {}
        self.alert_cooldown = 300     # 5分钟冷却
        self.active_pool: List[str] = []

    def refresh_active_pool(self) -> List[str]:
        """刷新活跃股票池 - 成交额前N名的活跃股

        交易时间门控（2026-10-20 按用户决策显式化）：遵守原项目约定
        "只在交易时间内取数据运算"（新闻推送除外）——非交易时段
        datacenter.get_all_realtime_quotes 恒返空，此处直接跳过
        避免无效请求与日志噪音，保持上一池待盘中首轮自动补刷。
        """
        try:
            if not is_trading_time():
                logger.debug("Non-trading time, skip active pool refresh")
                return self.active_pool or []
            df = self.datacenter.get_all_realtime_quotes()
            if df.empty:
                return self.active_pool or []
            
            # 过滤ST、价格过低
            if 'name' in df.columns:
                df = df[~df['name'].str.contains('ST', na=False)]
            if 'price' in df.columns:
                df['price'] = pd.to_numeric(df['price'], errors='coerce')
                df = df[df['price'] >= self.min_price]
            
            # 按成交额排序取前N
            if 'volume' in df.columns:
                df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0)
                # 用成交量×价格估算成交额
                df['amount'] = df['volume'] * pd.to_numeric(df['price'], errors='coerce').fillna(0)
                df = df.sort_values('amount', ascending=False)
            
            self.active_pool = df.head(self.target_count)['code'].tolist()
            logger.info(f"Active pool refreshed: {len(self.active_pool)} stocks")
            return self.active_pool
        except Exception as e:
            logger.error(f"Refresh active pool error: {e}")
            return self.active_pool or []

    def take_snapshot(self) -> Dict[str, Dict]:
        """获取当前快照（类似main66.py的quotation.stocks()）"""
        try:
            if not self.active_pool:
                self.refresh_active_pool()
            
            quotes = self.datacenter.get_realtime_quote(self.active_pool)
            if quotes is None or quotes.empty:
                return self.last_snapshot
            
            snapshot = {}
            for _, row in quotes.iterrows():
                code = row.get('code', '')
                price = float(row.get('price', 0))
                if price <= 0:
                    continue
                snapshot[code] = {
                    'price': price,
                    'change_pct': float(row.get('change_pct', 0)),
                    'volume': float(row.get('volume', 0)),
                    'name': row.get('name', ''),
                }
            return snapshot
        except Exception as e:
            logger.error(f"Take snapshot error: {e}")
            return self.last_snapshot

    def detect_burst(self) -> List[Dict]:
        """检测起爆信号（基于连续快照对比）"""
        current_snapshot = self.take_snapshot()
        bursts = []

        for code, data in current_snapshot.items():
            prev = self.last_snapshot.get(code)
            if not prev or prev.get('price', 0) <= 0:
                continue

            # 计算瞬时涨速
            speed = (data['price'] - prev['price']) / prev['price'] * 100
            # 计算成交额变化
            amount_change = (data['volume'] - prev.get('volume', 0)) * data['price']

            # 多重核心过滤
            # 1) 涨速达标
            if speed < self.speed_threshold:
                continue
            # 2) 当前位置过滤（涨幅2%-7.5%）
            if not (self.min_gain <= data['change_pct'] <= self.max_gain):
                continue
            # 3) 冷却检查
            now = time.time()
            last_alert = self.alerted.get(code, 0)
            if now - last_alert < self.alert_cooldown:
                continue

            burst = {
                'code': code,
                'name': data['name'],
                'price': data['price'],
                'change_pct': round(data['change_pct'], 2),
                'speed': round(speed, 2),
                'amount_change': amount_change,
                'timestamp': datetime.now().isoformat(),
            }
            bursts.append(burst)
            self.alerted[code] = now
            logger.info(f"🚀 Burst: {data['name']}({code}) speed={speed:.2f}% change={data['change_pct']:.1f}%")

        # 更新快照
        self.last_snapshot = current_snapshot
        return bursts

    def format_burst_message(self, burst: Dict) -> str:
        """格式化起爆消息"""
        return (
            f"**当前价格**: {burst['price']}\n\n"
            f"**当前涨幅**: {burst['change_pct']}%\n\n"
            f"**瞬间涨速**: {burst['speed']}%\n\n"
            f"**成交变化**: {burst.get('amount_change', 0) / 10000:.0f}万"
        )

    def get_active_pool(self) -> List[str]:
        """获取活跃股票池"""
        return self.active_pool or self.refresh_active_pool()
