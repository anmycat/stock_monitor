"""P5-b：dump B 类 BLE001 每处上下文（try 体 + except 体），供逐处分级决策。

分级口径（prd/15 §3.2 阶段六）：
- 收窄：try 体错误可预期（KeyError/ValueError/具体 IO 异常）→ 改具体异常
- noqa：兜底语义确有必要（降级/跳过单项继续）→ # noqa: BLE001 理由
用法：python research/scripts/dump_ble001_context.py > /tmp/ble001_ctx.txt
"""
import ast
import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
RUFF = "/Users/hr.wh/.workbuddy/binaries/python/versions/3.13.12/bin/ruff"

out = subprocess.run(
    [RUFF, "check", "quant_engine", "--select", "BLE001", "--output-format", "concise"],
    capture_output=True, text=True, cwd=ROOT,
).stdout

locs = []
for line in out.splitlines():
    parts = line.split(":")
    if len(parts) >= 4 and "BLE001" in line:
        locs.append((parts[0], int(parts[1])))

by_file = {}
for f, ln in locs:
    by_file.setdefault(f, []).append(ln)

for f in sorted(by_file):
    path = ROOT / f
    lines = path.read_text(encoding="utf-8").splitlines()
    print(f"\n{'='*80}\n{f}  ({len(by_file[f])} 处)")
    for ln in sorted(by_file[f]):
        # 向上找 try（最多 30 行）
        try_start = None
        for i in range(ln - 1, max(-1, ln - 31), -1):
            if lines[i].strip().startswith("try:"):
                try_start = i + 1  # 1-based
                break
        print(f"\n--- L{ln} ---")
        if try_start:
            body = lines[try_start - 1:min(try_start + 7, len(lines))]
            for j, bl in enumerate(body):
                print(f"  T{try_start + j}: {bl}")
            if try_start + 7 < ln:
                print(f"  ...(省略 {ln - try_start - 7} 行)")
        print(f"  E{ln}: {lines[ln - 1]}")
        # except 体首行
        for j in range(ln, min(ln + 3, len(lines))):
            print(f"  X{j + 1}: {lines[j]}")
