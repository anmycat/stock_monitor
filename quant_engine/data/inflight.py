"""进程内 in-flight 请求合并（2026-10-25，research/docs/25 §5.2）

同键并发取数只放行一个真实请求，其余线程等待并复用它结果——避免
同进程多线程（API 线程池 / 未来并发任务）对同一代码重复冲击 TDX/东财。
与 /api/signals 单飞锁同思路，但语义是「等待复用」而非「快速失败」。

语义：
- leader 真实执行 fn()，结果（含 None）写入 slot 后广播
- follower 等待 leader 完成并复用其结果（不等则各自取数）
- leader 抛异常 → follower 自行取数（错误不传染；异常多为瞬时故障）
- 等待超时（默认 15s）→ follower 自行取数（leader 挂起不拖死别人）
"""
from __future__ import annotations

import threading

from loguru import logger

_UNSET = object()


class _Slot:
    """一次 in-flight 执行的等待槽位"""

    __slots__ = ('error', 'event', 'result')

    def __init__(self):
        self.event = threading.Event()
        self.result = _UNSET
        self.error: BaseException | None = None


class InflightMerger:
    """同键并发合并执行器（线程安全，单进程内有效）"""

    def __init__(self, timeout: float = 15.0):
        self._timeout = timeout
        self._lock = threading.Lock()
        self._slots: dict[str, _Slot] = {}
        self.stats = {'merged': 0, 'passed': 0, 'timeouts': 0, 'leader_errors': 0}

    def run(self, key: str, fn):
        """执行 fn()；同键并发时仅一个真实执行，其余复用其结果"""
        with self._lock:
            slot = self._slots.get(key)
            if slot is None:
                slot = _Slot()
                self._slots[key] = slot
                leader = True
            else:
                leader = False

        if leader:
            try:
                result = fn()
                slot.result = result
                return result
            except BaseException as e:  # 记录后广播，follower 自行决策
                slot.error = e
                self.stats['leader_errors'] += 1
                raise
            finally:
                with self._lock:
                    if self._slots.get(key) is slot:
                        del self._slots[key]
                slot.event.set()

        # follower：等待 leader 结果
        if slot.event.wait(timeout=self._timeout):
            if slot.error is None and slot.result is not _UNSET:
                self.stats['merged'] += 1
                return slot.result
            # leader 失败/无结果 → 自行取数（不传染 leader 的错误）
        else:
            self.stats['timeouts'] += 1
            logger.debug(f"In-flight等待超时({self._timeout}s)，回退自行取数: {key}")
        self.stats['passed'] += 1
        return fn()

    def inflight_count(self) -> int:
        """当前 in-flight 键数（测试/观测用）"""
        with self._lock:
            return len(self._slots)
