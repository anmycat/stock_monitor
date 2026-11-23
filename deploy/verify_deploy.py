#!/usr/bin/env python3
"""D79 部署一致性分级校验（部署脚本 §6.5 调用）。

D77 教训：rsync -a 只比「大小+mtime」不校验内容 → .env.example 静默不同步数月。
本脚本把上传范围内文件按产物类型分级，只有「代码/配置」做字节级硬门禁：

  A 类（代码）：.py/.sh/.rs/.js/.css/.html/.ts          → sha256 逐文件必须一致
  B 类（配置）：.toml/.lock/.example/.txt/.json/.yaml/.md → sha256 逐文件必须一致
  C 类（产物）：.so/.dylib（qerust 等）                   → 只验「存在」，不比字节
                （跨 OS 不可比：mac dylib ≠ ECS ELF，D78 实证）
  D 类（状态）：.env*/logs/*.db*/__pycache__/target/.git   → 完全跳过

用法：
  python3 deploy/verify_deploy.py manifest <root>            # 输出分级清单 JSON 到 stdout
  python3 deploy/verify_deploy.py compare <local.json> <remote.json>
                                                            # 分级比对；A/B 不一致 exit 2
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from typing import Any

# 上传范围（与 update_ecs.sh rsync 目标一致）
SCAN_ROOTS = ("quant_engine", "UX", "deploy", "rust_accel")
EXCLUDE_DIR_PARTS = {"__pycache__", ".git", ".ruff_cache", "logs", "node_modules",
                     ".venv", "coding"}
EXCLUDE_DIR_PREFIX = ("target",)  # 排除 target 及 target<随机后缀>/（cargo 带随机 CARGO_TARGET_DIR）
# 注意：_qerust/ 不在此列——C 类产物（qerust.so）需计入「远端存在性」校验
# 只排除真密钥文件；.env.example 是模板文档，必须纳入 B 类硬门禁（D77 静默不同步教训）
EXCLUDE_ENV_NAMES = {".env", ".env.local", ".env.test"}
EXCLUDE_ENV_PREFIX = (".env.bak",)
EXCLUDE_SUFFIX_D = {".pyc", ".log", ".db", ".db-wal", ".db-shm", ".ds_store"}

TIER_A_SUFFIX = {".py", ".sh", ".rs", ".js", ".jsx", ".ts", ".tsx", ".css", ".html", ".htm"}
TIER_B_SUFFIX = {".toml", ".lock", ".txt", ".json", ".yaml", ".yml", ".md", ".example"}
TIER_C_SUFFIX = {".so", ".dylib", ".dll", ".pyd"}


def classify(path: str | Path) -> str | None:
    """返回 A/B/C/D；D 与目录外文件返回 None（不进清单）。接受 str 或 Path。"""
    parts = Path(path).parts
    name = parts[-1]
    is_secret_env = name in EXCLUDE_ENV_NAMES or name.startswith(EXCLUDE_ENV_PREFIX)
    if is_secret_env or name.lower().endswith(".ds_store"):
        return None
    if any(part in EXCLUDE_DIR_PARTS or part.startswith(EXCLUDE_DIR_PREFIX) for part in parts):
        return None
    suffix = Path(name).suffix.lower()
    if suffix in EXCLUDE_SUFFIX_D:
        return None
    if suffix in TIER_C_SUFFIX:
        return "C"
    if suffix in TIER_A_SUFFIX:
        return "A"
    if suffix in TIER_B_SUFFIX or name.endswith(".example"):
        return "B"
    # 无扩展名但在扫描范围内的脚本/配置（如 Makefile）→ B 保守纳入
    return "B" if name else None


def build_manifest(root: str | Path) -> dict[str, Any]:
    """扫描 root 下 SCAN_ROOTS，生成分级清单。"""
    root = Path(root)
    tiers: dict[str, dict[str, str]] = {"A": {}, "B": {}, "C": {}}
    skipped = 0
    for scan in SCAN_ROOTS:
        base = root / scan
        if not base.exists():
            continue
        for p in sorted(base.rglob("*")):
            if not p.is_file():
                continue
            rel = str(p.relative_to(root))
            tier = classify(rel)
            if tier is None:
                skipped += 1
                continue
            if tier == "C":
                tiers["C"][rel] = "present"  # 只记存在，不记 hash
            else:
                tiers[tier][rel] = hashlib.sha256(p.read_bytes()).hexdigest()
    return {"tiers": tiers, "skipped": skipped}


def compare(local: dict[str, Any], remote: dict[str, Any]) -> tuple[bool, list[str]]:
    """分级比对。A/B 严格（缺失+hash 不同都算失败）；C 只验远端存在。返回 (ok, 报告行)。"""
    lines: list[str] = []
    ok = True
    for tier in ("A", "B"):
        lmap = local["tiers"][tier]
        rmap = remote["tiers"][tier]
        only_local = sorted(set(lmap) - set(rmap))
        only_remote = sorted(set(rmap) - set(lmap))
        changed = sorted(k for k in set(lmap) & set(rmap) if lmap[k] != rmap[k])
        status = "OK" if not (only_local or only_remote or changed) else "FAIL"
        if status == "FAIL":
            ok = False
        lines.append(f"  [{tier} 类/{'代码' if tier == 'A' else '配置'}] {status}: "
                     f"{len(lmap)} 文件（本地）/ {len(rmap)}（远端），"
                     f"仅本地 {len(only_local)} / 仅远端 {len(only_remote)} / 内容不同 {len(changed)}")
        for k in only_local[:5]:
            lines.append(f"      - 仅本地: {k}")
        for k in only_remote[:5]:
            lines.append(f"      + 仅远端: {k}")
        for k in changed[:5]:
            lines.append(f"      ~ 内容不同: {k}")

    lmap_c = local["tiers"]["C"]
    rmap_c = remote["tiers"]["C"]
    missing = sorted(set(lmap_c) - set(rmap_c))
    lines.append(f"  [C 类/产物] {'FAIL' if missing else 'OK'}: "
                 f"远端产物 {len(rmap_c)} 项，本地 {len(lmap_c)} 项；远端缺失 {len(missing)}"
                 "（只验存在，不比字节——跨 OS 不可比）")
    for k in missing[:5]:
        lines.append(f"      - 远端缺失: {k}")
    if missing:
        ok = False
    return ok, lines


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(__doc__)
        return 1
    cmd = argv[1]
    if cmd == "manifest":
        if len(argv) != 3:
            print("用法: verify_deploy.py manifest <root>", file=sys.stderr)
            return 1
        print(json.dumps(build_manifest(argv[2]), ensure_ascii=False, sort_keys=True))
        return 0
    if cmd == "compare":
        if len(argv) != 4:
            print("用法: verify_deploy.py compare <local.json> <remote.json>", file=sys.stderr)
            return 1
        local = json.loads(Path(argv[2]).read_text())
        remote = json.loads(Path(argv[3]).read_text())
        ok, lines = compare(local, remote)
        print("== 部署一致性分级校验（A/B 字节级硬门禁，C 只验存在，D 跳过）==")
        for line in lines:
            print(line)
        print("  结果:", "✅ 通过" if ok else "❌ 失败")
        return 0 if ok else 2
    print(f"未知命令: {cmd}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
