"""P3 验收：API 冒烟 4 端点（TestClient 内存内，不依赖外部端口）。

口径（prd/15 §3.2 每阶段通用验收）：/api/health /api/market /api/signals
/api/evaluation/runs 全部 200。
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))  # 仓库根

from fastapi.testclient import TestClient

from quant_engine.main import app

client = TestClient(app)
ENDPOINTS = [
    "/api/health",
    "/api/market",
    "/api/signals",
    "/api/evaluation/runs",
]
failed = 0
for ep in ENDPOINTS:
    try:
        r = client.get(ep, timeout=120)
        ok = r.status_code == 200
        print(f"{'PASS' if ok else 'FAIL'} {ep} -> {r.status_code}")
        if not ok:
            failed += 1
            print(f"  body[:300]: {r.text[:300]}")
    except Exception as e:
        failed += 1
        print(f"FAIL {ep} -> EXC {type(e).__name__}: {e}")
sys.exit(1 if failed else 0)
