"""抓取硬超时防护（M4）

背景（prd/07 §15 B 线 M4）：新闻源 / ETF 持仓源等外网抓取若卡死
（对端不响应、连接挂起），会拖垮 worker 主循环。裸 requests 超时
只覆盖连接/读取阶段，无法应对 DNS 挂起、TLS 僵死等场景。

方案：子进程 + Pipe 回传 + 父进程硬超时 kill。
- 每次抓取 fork 独立子进程执行，结果（或异常）经 Pipe pickle 回传
- 父进程 join(timeout) 超时即 terminate，2s 宽限后仍活则 kill
- BoundedSemaphore 限制并发子进程数，防多源同时拉起打爆机器

注意：func 与返回值必须可 pickle（DataFrame / 基本类型 / dataclass 均可）。
"""

from __future__ import annotations

import multiprocessing as mp
import threading
import time
from contextlib import suppress

from loguru import logger

# 并发子进程上限（新闻 6 源 + ETF 同时触发的最坏情况）
_MAX_CONCURRENT = 4
_semaphore = threading.BoundedSemaphore(_MAX_CONCURRENT)

# fork 上下文：Linux/macOS 均可用；spawn 平台会退化为直调（见 _spawn_ok）
try:
    _ctx = mp.get_context('fork')
    _FORK_OK = True
except ValueError:  # pragma: no cover - Windows 无 fork
    _ctx = None
    _FORK_OK = False


def _worker(conn, func, args, kwargs):
    """子进程入口：执行 func，结果或异常经 Pipe 回传。"""
    try:
        result = func(*args, **kwargs)
        conn.send((True, result))
    except BaseException as e:  # noqa: BLE001 子进程内兜底，全部回传父进程
        # Pipe 已断（父进程超时退出）则静默
        with suppress(Exception):
            conn.send((False, f'{type(e).__name__}: {e}'))
    finally:
        conn.close()


def guarded_fetch(func, *args, timeout: float = 10.0, name: str = 'fetch', **kwargs):
    """带硬超时的同步抓取。

    超时/被杀返回 None（与既有抓取失败语义一致，调用方按空数据处理）。
    子进程不可用（非 fork 平台）时退化为直接调用（仍受 requests 自身超时保护）。
    """
    if not _FORK_OK:  # pragma: no cover - 非 Unix 平台
        return func(*args, **kwargs)

    acquired = _semaphore.acquire(timeout=timeout + 5)
    if not acquired:
        logger.warning(f"[FetchGuard] {name} 等待并发槽超时，跳过本轮")
        return None

    proc = None
    try:
        parent_conn, child_conn = _ctx.Pipe(duplex=False)
        proc = _ctx.Process(
            target=_worker, args=(child_conn, func, args, kwargs),
            name=f'fetch-guard-{name}', daemon=True,
        )
        proc.start()
        child_conn.close()  # 父端只读

        deadline = time.monotonic() + timeout
        payload = None
        while time.monotonic() < deadline:
            if parent_conn.poll(0.2):
                try:
                    payload = parent_conn.recv()
                except EOFError:
                    payload = (False, 'pipe EOF')
                break
            if not proc.is_alive() and not parent_conn.poll(0):
                # 进程已退出但无回传（子进程被外部 kill 或 send 失败）
                payload = (False, 'child exited without payload')
                break

        if payload is None:
            logger.warning(f"[FetchGuard] {name} 超过 {timeout}s 未返回，强制终止")
            _kill(proc)
            return None

        ok, value = payload
        if not ok:
            logger.warning(f"[FetchGuard] {name} 子进程失败: {value}")
            return None
        return value
    finally:
        if proc is not None and proc.is_alive():
            _kill(proc)
        with suppress(Exception):  # 关闭已断连接
            parent_conn.close()
        _semaphore.release()


def _kill(proc) -> None:
    """两级处决：terminate（SIGTERM）→ 2s 宽限 → kill（SIGKILL）。"""
    with suppress(Exception):  # 进程已退出等
        proc.terminate()
        proc.join(2)
    if proc.is_alive():
        with suppress(Exception):  # 兜底
            proc.kill()
            proc.join(1)


def guarded(timeout: float = 10.0, name: str | None = None):
    """方法装饰器版本：把被装饰方法的调用包进子进程硬超时。

    用于源对象的 fetch()：self 不可跨进程序列化，因此包装时改为
    以 (未绑定方法, self, *args) 形式调用——self 随子进程 fork 继承，
    不走 pickle。
    """

    def decorator(method):
        def wrapper(self, *args, **kwargs):
            fn_name = name or f'{type(self).__name__}.{method.__name__}'
            return guarded_fetch(method, self, *args, timeout=timeout, name=fn_name, **kwargs)
        wrapper.__name__ = method.__name__
        wrapper.__doc__ = method.__doc__
        return wrapper

    return decorator
