"""
WebSocket连接管理器 - 增强版支持服务器主动Push
优化: 增量更新 + 批量推送 + 消息优先级 + AI预测活跃度
"""
import json
import gzip
import base64
import asyncio
import heapq
import re
import uuid
from datetime import datetime
from enum import IntEnum
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
from typing import List, Dict, Optional, Any
from loguru import logger
from .dependencies import signal_generator, datacenter, etf_monitor, sector_monitor, market_alert, hot_money_monitor
from .auth import verify_websocket_token
from ..services.market.market_analyzer import MarketAnalyzer

ws_router = APIRouter()


class MessagePriority(IntEnum):
    """消息优先级（数值越小优先级越高）"""
    ALERT = 0          # 异动: 最高优先级
    MARKET = 1         # 市场数据: 高优先级
    SIGNAL = 2         # 信号: 中优先级
    ETF = 3            # ETF: 低优先级
    SECTOR = 4         # 板块: 低优先级
    HOT_MONEY = 5      # 游资: 低优先级
    AI_SUMMARY = 0     # AI总结: 最高优先级


class MessageQueue:
    """优先级消息队列 - 支持批量推送"""
    
    def __init__(self, batch_interval: float = 2.0, max_batch_size: int = 10):
        self._queue: List[tuple] = []  # heapq: (priority, timestamp, message)
        self._counter = 0
        self.batch_interval = batch_interval  # 批量推送间隔（秒）
        self.max_batch_size = max_batch_size   # 最大批量大小
    
    def push(self, message: Dict, priority: MessagePriority):
        """入队消息"""
        heapq.heappush(self._queue, (int(priority), self._counter, message))
        self._counter += 1
    
    def pop_batch(self) -> List[Dict]:
        """批量出队（按优先级排序）"""
        batch = []
        while self._queue and len(batch) < self.max_batch_size:
            _, _, msg = heapq.heappop(self._queue)
            batch.append(msg)
        return batch
    
    def is_empty(self) -> bool:
        return len(self._queue) == 0
    
    @property
    def size(self) -> int:
        return len(self._queue)


class ConnectionManager:
    """WebSocket连接管理器 - 支持服务器主动Push"""
    
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.push_task: Optional[asyncio.Task] = None
        self.market_analyzer = MarketAnalyzer(datacenter)
        self.last_push_time = {}
        
        # 基础Push间隔（秒）
        self.base_push_intervals = {
            'market': 60,        # 市场数据: 60秒
            'signals': 300,      # 信号: 5分钟
            'alerts': 30,        # 异动: 30秒
            'etf': 60,           # ETF: 60秒
            'sectors': 120,      # 板块: 2分钟
            'hot_money': 120,    # 游资: 2分钟
        }
        
        # 当前Push间隔（会根据市场活跃度动态调整）
        self.push_intervals = self.base_push_intervals.copy()
        
        # 市场活跃度监控
        self.market_activity_history = []  # 记录最近20次市场变化率
        self.activity_window = 20  # 滑动窗口大小
        
        # Gzip压缩配置
        self.compression_enabled = True
        self.compression_threshold = 1024  # 超过1KB的消息才压缩
        self.compression_stats = {
            'total_messages': 0,
            'compressed_messages': 0,
            'original_bytes': 0,
            'compressed_bytes': 0
        }
        
        # === 增量更新：上次推送的数据缓存 ===
        self._last_data: Dict[str, Any] = {}
        
        # === 批量推送：优先级消息队列 ===
        self.msg_queue = MessageQueue(batch_interval=2.0, max_batch_size=10)
        self.last_batch_flush = 0
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"✅ WebSocket客户端已连接. 总连接数: {len(self.active_connections)}")
        
        # 启动主动Push任务（如果还没有）
        if self.push_task is None or self.push_task.done():
            self.push_task = asyncio.create_task(self._auto_push_loop())
            logger.info("🚀 服务器主动Push任务已启动")
    
    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info(f"❌ WebSocket客户端已断开. 总连接数: {len(self.active_connections)}")
    
    async def send_message(self, message: str, websocket: WebSocket):
        try:
            await websocket.send_text(message)
        except Exception as e:
            logger.error(f"发送消息失败: {e}")
            self.disconnect(websocket)
    
    async def broadcast(self, message: str):
        """广播消息到所有连接（支持Gzip压缩）"""
        # 统计原始大小
        original_size = len(message.encode('utf-8'))
        self.compression_stats['total_messages'] += 1
        self.compression_stats['original_bytes'] += original_size
        
        # 判断是否需要压缩
        if self.compression_enabled and original_size > self.compression_threshold:
            try:
                # Gzip压缩
                compressed = gzip.compress(message.encode('utf-8'), compresslevel=6)
                encoded = base64.b64encode(compressed).decode('ascii')
                
                # 构造压缩消息
                compressed_msg = json.dumps({
                    '_compressed': True,
                    'data': encoded,
                    'original_size': original_size
                })
                
                compressed_size = len(compressed_msg.encode('utf-8'))
                self.compression_stats['compressed_messages'] += 1
                self.compression_stats['compressed_bytes'] += compressed_size
                
                # 日志统计（每100条消息输出一次）
                if self.compression_stats['total_messages'] % 100 == 0:
                    ratio = (1 - self.compression_stats['compressed_bytes'] / self.compression_stats['original_bytes']) * 100
                    logger.info(f"📦 压缩统计: 总消息={self.compression_stats['total_messages']}, "
                              f"压缩消息={self.compression_stats['compressed_messages']}, "
                              f"压缩率={ratio:.1f}%")
                
                message_to_send = compressed_msg
            except Exception as e:
                logger.warning(f"压缩失败，使用原始消息: {e}")
                message_to_send = message
        else:
            message_to_send = message
        
        # 广播消息
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_text(message_to_send)
            except Exception:
                disconnected.append(connection)
        
        # 清理断开的连接
        for conn in disconnected:
            self.disconnect(conn)
    
    async def _auto_push_loop(self):
        """主动Push循环 - 增量更新 + 批量推送 + 消息优先级 + AI预测"""
        logger.info("🔄 主动Push循环已启动 (v3: 增量+批量+优先级+AI预测)")
        
        last_activity_check = 0
        
        while self.active_connections:
            try:
                now = datetime.now()
                current_time = now.timestamp()
                
                # 每60秒检查一次市场活跃度，调整Push频率
                if current_time - last_activity_check >= 60:
                    activity = self._calculate_market_activity()
                    self._adjust_push_frequency()
                    # AI预测下一周期活跃度
                    predicted = self._predict_activity()
                    if abs(predicted - activity) > 15:
                        logger.info(f"🧠 AI活跃度预测: 当前={activity:.0f}, 预测={predicted:.0f}")
                    last_activity_check = current_time
                
                # === 按优先级采集数据并入队 ===
                
                # 1. 异动检测（最高优先级，立即入队）
                if current_time - self.last_push_time.get('alerts', 0) >= self.push_intervals['alerts']:
                    await self._enqueue_alerts(now)
                    self.last_push_time['alerts'] = current_time
                
                # 2. 市场数据（高优先级，增量更新）
                if current_time - self.last_push_time.get('market', 0) >= self.push_intervals['market']:
                    await self._enqueue_market_incremental(now)
                    self.last_push_time['market'] = current_time
                
                # 3. 交易信号（中优先级）
                if current_time - self.last_push_time.get('signals', 0) >= self.push_intervals['signals']:
                    await self._enqueue_signals(now)
                    self.last_push_time['signals'] = current_time
                
                # 4. ETF（低优先级）
                if current_time - self.last_push_time.get('etf', 0) >= self.push_intervals['etf']:
                    await self._enqueue_etf()
                    self.last_push_time['etf'] = current_time
                
                # 5. 板块（低优先级）
                if current_time - self.last_push_time.get('sectors', 0) >= self.push_intervals['sectors']:
                    await self._enqueue_sectors()
                    self.last_push_time['sectors'] = current_time
                
                # 6. 游资（低优先级）
                if current_time - self.last_push_time.get('hot_money', 0) >= self.push_intervals['hot_money']:
                    await self._enqueue_hot_money()
                    self.last_push_time['hot_money'] = current_time
                
                # === 批量刷新队列（每2秒） ===
                if current_time - self.last_batch_flush >= self.msg_queue.batch_interval:
                    await self._flush_message_queue()
                    self.last_batch_flush = current_time
                
                # 等待2秒（批量推送间隔更短）
                await asyncio.sleep(2)
                
            except Exception as e:
                logger.error(f"主动Push循环错误: {e}")
                await asyncio.sleep(10)
        
        logger.info("🛑 主动Push循环已停止（无活跃连接）")
    
    async def _flush_message_queue(self):
        """批量刷新消息队列"""
        if self.msg_queue.is_empty():
            return
        
        batch = self.msg_queue.pop_batch()
        if not batch:
            return
        
        # 构造批量消息
        batch_msg = {
            'type': 'batch',
            'data': batch,
            'count': len(batch),
            'timestamp': datetime.now().isoformat()
        }
        
        await self.broadcast(json.dumps(batch_msg, default=str))
        logger.debug(f"📨 批量推送: {len(batch)}条消息")
    
    # === 增量更新 + 优先级入队 ===
    
    async def _enqueue_alerts(self, now):
        """异动检测 - 最高优先级"""
        try:
            from ..utils.calendar import is_trading_day
            if not is_trading_day(now):
                return
            h, m = now.hour, now.minute
            t = h * 60 + m
            is_trading = (9*60+30 <= t < 11*60+30) or (13*60 <= t < 15*60)
            if not is_trading:
                return
            
            market = self.market_analyzer.analyze_market()
            alerts = market_alert.detect_alerts(market)
            
            if alerts:
                for alert in alerts:
                    self.msg_queue.push({
                        'type': 'alert',
                        'data': alert
                    }, MessagePriority.ALERT)
                logger.info(f"📢 异动入队: {len(alerts)}条 (优先级={MessagePriority.ALERT})")
        except Exception as e:
            logger.error(f"异动检测失败: {e}")
    
    async def _enqueue_market_incremental(self, now):
        """市场数据 - 增量更新（只推送变化的字段）"""
        try:
            from ..utils.calendar import is_trading_day
            if not is_trading_day(now):
                return
            h, m = now.hour, now.minute
            t = h * 60 + m
            is_trading = (9*60+30 <= t < 11*60+30) or (13*60 <= t < 15*60)
            is_after_close = t >= 15*60 and t < 15*60+30
            if not is_trading and not is_after_close:
                return
            
            sentiment = etf_monitor.get_market_sentiment()
            is_closed = t >= 15*60
            
            # 当前全量数据
            current = {
                'up_count': sentiment.get('up_count', 0),
                'down_count': sentiment.get('down_count', 0),
                'sentiment': sentiment.get('sentiment', 'neutral'),
                'score': sentiment.get('score', 50),
                'avg_change': sentiment.get('avg_change', 0),
                'market_type': '已收盘' if is_closed else sentiment.get('sentiment', 'unknown'),
                'is_closed': is_closed,
                'timestamp': now.isoformat()
            }
            
            # 增量比较：只推送变化的字段
            last = self._last_data.get('market', {})
            delta = {}
            for key, val in current.items():
                if key not in last or last[key] != val:
                    delta[key] = val
            
            if not delta:
                return  # 数据无变化，不推送
            
            # 缓存当前数据
            self._last_data['market'] = current
            
            self.msg_queue.push({
                'type': 'market_update',
                'data': delta,
                '_incremental': True  # 标记为增量更新
            }, MessagePriority.MARKET)
            
        except Exception as e:
            logger.error(f"市场数据增量更新失败: {e}")
    
    async def _enqueue_signals(self, now):
        """交易信号 - 中优先级"""
        try:
            from ..utils.calendar import is_trading_day
            if not is_trading_day(now):
                return
            h, m = now.hour, now.minute
            t = h * 60 + m
            is_trading = (9*60+30 <= t < 11*60+30) or (13*60 <= t < 15*60)
            if not is_trading:
                return
            
            signals = signal_generator.scan_all(max_signals=3)
            if signals:
                # 增量比较
                sig_codes = [s.code for s in signals]
                last_codes = self._last_data.get('signal_codes', [])
                if sig_codes == last_codes:
                    return  # 信号无变化
                
                self._last_data['signal_codes'] = sig_codes
                
                self.msg_queue.push({
                    'type': 'signals',
                    'data': [
                        {
                            'code': s.code, 'name': s.name,
                            'action': s.action.value,
                            'level': s.level.value if s.level else None,
                            'score': s.score, 'position': s.position,
                            'reason': s.reason, 'strategy': s.strategy,
                            'price': s.price,
                        }
                        for s in signals
                    ]
                }, MessagePriority.SIGNAL)
                logger.info(f"📈 信号入队: {len(signals)}个")
        except Exception as e:
            logger.error(f"信号推送失败: {e}")
    
    async def _enqueue_etf(self):
        """ETF数据 - 低优先级"""
        try:
            etf_list = etf_monitor.get_etf_status()
            if etf_list:
                self.msg_queue.push({
                    'type': 'etf_update',
                    'data': etf_list[:10]
                }, MessagePriority.ETF)
        except Exception as e:
            logger.error(f"ETF入队失败: {e}")
    
    async def _enqueue_sectors(self):
        """板块数据 - 低优先级"""
        try:
            sectors = sector_monitor.get_sector_data()
            if sectors:
                self.msg_queue.push({
                    'type': 'sector_update',
                    'data': sectors[:8]
                }, MessagePriority.SECTOR)
        except Exception as e:
            logger.error(f"板块入队失败: {e}")
    
    async def _enqueue_hot_money(self):
        """游资数据 - 低优先级"""
        try:
            hot_data = hot_money_monitor.get_hot_money_activity()
            if hot_data:
                self.msg_queue.push({
                    'type': 'hot_money_update',
                    'data': hot_data
                }, MessagePriority.HOT_MONEY)
        except Exception as e:
            logger.error(f"游资入队失败: {e}")
    
    async def broadcast_ai_summary(self, summary_data: Dict):
        """广播AI总结（最高优先级，立即推送）"""
        self.msg_queue.push({
            'type': 'ai_summary',
            'data': summary_data
        }, MessagePriority.AI_SUMMARY)
        # 立即刷新
        await self._flush_message_queue()
        logger.info("🤖 AI总结已入队并刷新 (优先级=0)")
    
    def _predict_activity(self) -> float:
        """
        AI预测下一周期活跃度（基于历史数据的简单线性回归）
        
        使用最近20次活跃度数据拟合趋势线，预测下一个值。
        如果历史数据不足，返回当前值。
        """
        if len(self.market_activity_history) < 3:
            return self.market_activity_history[-1] if self.market_activity_history else 50.0
        
        try:
            # 简单线性回归: y = a*x + b
            n = len(self.market_activity_history)
            xs = list(range(n))
            ys = self.market_activity_history
            
            x_mean = sum(xs) / n
            y_mean = sum(ys) / n
            
            numerator = sum((xs[i] - x_mean) * (ys[i] - y_mean) for i in range(n))
            denominator = sum((xs[i] - x_mean) ** 2 for i in range(n))
            
            if denominator == 0:
                return y_mean
            
            slope = numerator / denominator
            intercept = y_mean - slope * x_mean
            
            # 预测下一个点
            predicted = slope * n + intercept
            # 限制在0-100范围
            return max(0, min(100, predicted))
            
        except Exception as e:
            logger.error(f"AI活跃度预测失败: {e}")
            return 50.0
    
    def _calculate_market_activity(self) -> float:
        """
        计算市场活跃度（0-100）
        
        基于以下指标：
        1. 涨跌家数变化率
        2. ETF涨跌幅绝对值
        3. 异动数量
        
        Returns:
            float: 活跃度分数（0-100）
        """
        try:
            from ..utils.calendar import is_trading_day
            from datetime import datetime as dt
            
            now = dt.now()
            if not is_trading_day(now):
                return 0.0  # 非交易日
            
            h, m = now.hour, now.minute
            t = h * 60 + m
            
            # 非交易时间
            is_trading = (9*60+30 <= t < 11*60+30) or (13*60 <= t < 15*60)
            if not is_trading:
                return 0.0
            
            # 获取市场数据
            sentiment = etf_monitor.get_market_sentiment()
            up_count = sentiment.get('up_count', 0)
            down_count = sentiment.get('down_count', 0)
            avg_change = abs(sentiment.get('avg_change', 0))
            
            # 指标1: 涨跌家数变化（相对变化率）
            total = up_count + down_count
            if total > 0:
                change_ratio = abs(up_count - down_count) / total  # 0-1
            else:
                change_ratio = 0
            
            # 指标2: 平均涨跌幅绝对值
            # 通常0-5%，转换为0-1
            change_score = min(avg_change / 5.0, 1.0)
            
            # 指标3: 近期异动数量
            alerts = market_alert.get_history(5)  # 最近5次异动
            alert_score = min(len(alerts) / 5.0, 1.0)
            
            # 综合活跃度（加权平均）
            activity = (
                change_ratio * 0.4 +      # 涨跌变化 40%
                change_score * 0.4 +      # 涨跌幅 40%
                alert_score * 0.2         # 异动 20%
            ) * 100
            
            # 记录到历史
            self.market_activity_history.append(activity)
            if len(self.market_activity_history) > self.activity_window:
                self.market_activity_history.pop(0)
            
            return activity
            
        except Exception as e:
            logger.error(f"计算市场活跃度失败: {e}")
            return 50.0  # 默认中等活跃度
    
    def _adjust_push_frequency(self):
        """
        根据市场活跃度动态调整Push频率
        
        策略：
        - 高活跃度 (>70): 提高频率（间隔缩短50%）
        - 中等活跃度 (30-70): 保持基础频率
        - 低活跃度 (<30): 降低频率（间隔延长100%）
        """
        try:
            if not self.market_activity_history:
                return
            
            # 计算平均活跃度
            avg_activity = sum(self.market_activity_history) / len(self.market_activity_history)
            
            # 根据活跃度调整
            if avg_activity > 70:
                # 高活跃度：缩短间隔50%
                multiplier = 0.5
                level = "🔴 高活跃"
            elif avg_activity > 50:
                # 中高活跃度：缩短间隔25%
                multiplier = 0.75
                level = "🟠 中高活跃"
            elif avg_activity > 30:
                # 中等活跃度：保持基础
                multiplier = 1.0
                level = "🟡 中等活跃"
            else:
                # 低活跃度：延长间隔100%
                multiplier = 2.0
                level = "🟢 低活跃"
            
            # 应用新的间隔
            new_intervals = {}
            for key, base_interval in self.base_push_intervals.items():
                new_interval = int(base_interval * multiplier)
                # 限制范围：最小10秒，最大600秒
                new_interval = max(10, min(new_interval, 600))
                new_intervals[key] = new_interval
            
            # 检查是否有变化
            if new_intervals != self.push_intervals:
                old_market = self.push_intervals.get('market', 60)
                new_market = new_intervals.get('market', 60)
                
                self.push_intervals = new_intervals
                
                logger.info(f"📊 Push频率调整: {level} (活跃度={avg_activity:.1f})")
                logger.info(f"   市场数据: {old_market}s -> {new_market}s")
                logger.info(f"   异动检测: {self.push_intervals['alerts']}s")
                logger.info(f"   交易信号: {self.push_intervals['signals']}s")
            
        except Exception as e:
            logger.error(f"调整Push频率失败: {e}")


manager = ConnectionManager()


@ws_router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, token: str = Query(None)):
    """WebSocket端点"""
    connection_id = f"ws-{str(uuid.uuid4())[:8]}"
    with logger.contextualize(request_id=connection_id):
        logger.info("WebSocket连接请求")
        # 验证Token（如果有配置）
        if not verify_websocket_token(token):
            await websocket.close(code=4001, reason="Invalid or missing token")
            logger.warning("WebSocket连接被拒绝: 无效的Token")
            return
        
        await manager.connect(websocket)
        
        try:
            # 发送欢迎消息
            welcome_msg = {
                'type': 'welcome',
                'data': {
                    'message': '欢迎连接量化交易系统',
                    'timestamp': datetime.now().isoformat(),
                    'push_enabled': True
                }
            }
            await manager.send_message(json.dumps(welcome_msg), websocket)
            
            # 等待客户端消息（保持连接）
            while True:
                data = await websocket.receive_text()
                try:
                    message = json.loads(data)
                    message_type = message.get('type')
                    
                    # 处理客户端请求
                    if message_type == 'scan':
                        max_signals = message.get('max_signals', 3)
                        if not isinstance(max_signals, int):
                            raise ValueError('max_signals必须是整数')
                        if max_signals < 1 or max_signals > 10:
                            raise ValueError('max_signals范围应为1-10')
                        signals = signal_generator.scan_all(max_signals)
                        response = {
                            'type': 'signals',
                            'data': [
                                {
                                    'code': s.code,
                                    'name': s.name,
                                    'action': s.action.value,
                                    'level': s.level.value if s.level else None,
                                    'score': s.score,
                                    'position': s.position,
                                    'reason': s.reason,
                                    'strategy': s.strategy,
                                    'price': s.price,
                                }
                                for s in signals
                            ]
                        }
                        await manager.send_message(json.dumps(response, default=str), websocket)
                    
                    elif message_type == 'scan_stock':
                        code = message.get('code')
                        if not isinstance(code, str) or not re.match(r'^\d{6}\.(SZ|SH|BJ)$', code):
                            raise ValueError('无效股票代码格式')
                        signal = signal_generator.scan_stock(code)
                        response = {
                            'type': 'stock_signal',
                            'data': {
                                'code': signal.code,
                                'name': signal.name,
                                'action': signal.action.value,
                                'level': signal.level.value if signal.level else None,
                                'score': signal.score,
                                'position': signal.position,
                                'reason': signal.reason,
                                'strategy': signal.strategy,
                                'price': signal.price,
                            } if signal else None
                        }
                        await manager.send_message(json.dumps(response, default=str), websocket)
                    
                    elif message_type == 'ping':
                        # 心跳响应
                        pong = {'type': 'pong', 'timestamp': datetime.now().isoformat()}
                        await manager.send_message(json.dumps(pong), websocket)

                    elif message_type == 'chat':
                        # Agent策略问股
                        user_msg = message.get('message', '')
                        session_id = message.get('session_id', 'default')
                        if not isinstance(user_msg, str) or not user_msg.strip() or len(user_msg) > 2000:
                            raise ValueError('message长度应为1-2000')
                        if not isinstance(session_id, str) or not re.match(r'^[A-Za-z0-9_-]{1,64}$', session_id):
                            raise ValueError('无效session_id')
                        try:
                            from ..services.ai.agent_router import route_message
                            result = await route_message(user_msg, session_id, datacenter, signal_generator)
                            await manager.send_message(json.dumps(result, default=str), websocket)
                        except Exception as e:
                            logger.error(f"Chat error: {e}")
                            await manager.send_message(
                                json.dumps({'type': 'chat', 'text': '处理出错'}),
                                websocket
                            )
                    else:
                        await manager.send_message(
                            json.dumps({'type': 'error', 'message': 'Unsupported message type'}),
                            websocket
                        )
                        
                except json.JSONDecodeError:
                    await manager.send_message(
                        json.dumps({'type': 'error', 'message': 'Invalid JSON'}), 
                        websocket
                    )
                except ValueError as e:
                    await manager.send_message(
                        json.dumps({'type': 'error', 'message': str(e)}),
                        websocket
                    )
                    
        except WebSocketDisconnect:
            manager.disconnect(websocket)
        except Exception as e:
            logger.error(f"WebSocket错误: {e}")
            manager.disconnect(websocket)
