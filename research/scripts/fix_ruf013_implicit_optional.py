"""P5-f：RUF013 implicit-optional 批量修复（`def f(x: int = None)` → `x: int | None = None`）。

安全前提：目标文件必须有 `from __future__ import annotations`（PEP 563），
否则 py3.9 定义时求值 X | None 会 TypeError。脚本自动检查，缺 future 的文件
先补（复用 add_future_annotations 逻辑）。
用法：python research/scripts/fix_ruf013_implicit_optional.py [--dry-run]
"""
import ast
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
FUTURE = "from __future__ import annotations"


def ensure_future(text: str) -> str:
    if FUTURE in text:
        return text
    lines = text.splitlines()
    idx = 0
    if lines and (lines[0].startswith('"""') or lines[0].startswith("'''")):
        quote = lines[0][:3]
        if len(lines[0]) > 3 and lines[0].endswith(quote):
            idx = 1
        else:
            for i in range(1, len(lines)):
                if quote in lines[i]:
                    idx = i + 1
                    break
    while idx < len(lines) and not lines[idx].strip():
        idx += 1
    lines.insert(idx, FUTURE)
    return "\n".join(lines) + "\n"


def fix_file(path: pathlib.Path, dry: bool) -> int:
    src = path.read_text(encoding="utf-8")
    tree = ast.parse(src)
    edits = []  # (lineno, col, old_segment, new_segment)
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        args = node.args
        all_args = list(args.posonlyargs) + list(args.args) + list(args.kwonlyargs)
        if args.vararg:
            all_args.append(args.vararg)
        if args.kwarg:
            all_args.append(args.kwarg)
        defaults = list(args.defaults) + [d for d in args.kw_defaults if d is not None]
        for arg in all_args:
            if arg.annotation is None:
                continue
            # 默认值为 None 常量（位置或关键字）
            is_none_default = False
            if arg in args.posonlyargs:
                di = args.posonlyargs.index(arg)
                if di < len(args.defaults):
                    is_none_default = isinstance(args.defaults[di], ast.Constant) and args.defaults[di].value is None
            elif arg in args.args:
                di = args.args.index(arg) - (len(args.args) - len(args.defaults))
                if di >= 0:
                    is_none_default = isinstance(args.defaults[di], ast.Constant) and args.defaults[di].value is None
            elif arg in args.kwonlyargs:
                ki = args.kwonlyargs.index(arg)
                d = args.kw_defaults[ki]
                is_none_default = d is not None and isinstance(d, ast.Constant) and d.value is None
            if not is_none_default:
                continue
            seg = ast.get_source_segment(src, arg.annotation)
            if seg is None or seg.startswith(("\"", "'")):
                continue  # 字符串注解跳过
            line = arg.annotation.lineno
            col = arg.annotation.col_offset
            edits.append((line, col, seg, f"{seg} | None"))
    if not edits:
        return 0
    lines = src.splitlines()
    # 行内倒序替换（同行多参数时从后往前）
    edits.sort(key=lambda e: (e[0], -e[1]))
    for lineno, col, old, new in edits:
        ln = lines[lineno - 1]
        if ln[col:col + len(old)] != old:
            print(f"  WARN {path.name}:{lineno} 段不匹配 {old!r}，跳过")
            continue
        lines[lineno - 1] = ln[:col] + new + ln[col + len(old):]
    if not dry:
        new_src = ensure_future("\n".join(lines) + "\n")
        path.write_text(new_src, encoding="utf-8")
    return len(edits)


def main():
    dry = "--dry-run" in sys.argv
    # ruff 输出精确驱动（最小改动原则：只修 ruff 判定的 RUF013 行）
    import subprocess
    ruff_out = subprocess.run(
        ["/Users/hr.wh/.workbuddy/binaries/python/versions/3.13.12/bin/ruff",
         "check", "quant_engine", "--select", "RUF013", "--output-format", "concise"],
        capture_output=True, text=True, cwd=ROOT,
    ).stdout
    targets = {}  # file -> set(lineno)
    for line in ruff_out.splitlines():
        parts = line.split(":")
        if len(parts) >= 4 and "RUF013" in line:
            targets.setdefault(parts[0], set()).add(int(parts[1]))
    total = 0
    for rel in sorted(targets):
        p = ROOT / rel
        src = p.read_text(encoding="utf-8")
        lines = src.splitlines()
        n = 0
        for ln in sorted(targets[rel], reverse=True):
            # ruff 报的行是参数所在行；在该行找 `name: Type = None` 模式
            m = re.search(r"(\w+)(\s*:\s*[^=]+?)(\s*=\s*None)", lines[ln - 1])
            if not m:
                print(f"  WARN {rel}:{ln} 无匹配模式: {lines[ln - 1]!r}")
                continue
            old = m.group(0)
            new = f"{m.group(1)}{m.group(2)} | None{m.group(3)}"
            lines[ln - 1] = lines[ln - 1].replace(old, new, 1)
            n += 1
        if n:
            if not dry:
                new_src = ensure_future("\n".join(lines) + "\n")
                p.write_text(new_src, encoding="utf-8")
            total += n
            print(f"{'DRY' if dry else 'FIX'} {rel}: {n} 处")
    print(f"\n{'would fix' if dry else 'fixed'}: {total} 处 / {len(targets)} 文件")


if __name__ == "__main__":
    main()
