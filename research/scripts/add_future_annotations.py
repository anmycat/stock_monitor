"""P5-e：为含 Optional 注解的 33 个文件加 `from __future__ import annotations`。

背景：本地 venv py3.9 下 `X | None`（PEP 604）在 pydantic 模型字段/函数注解
定义时即求值并抛 TypeError；生产 ECS 为 py3.10+ 无此问题，但测试基线必须
在 3.9 通过。PEP 563 字符串化注解后 UP045 安全（pydantic v2 内部
get_type_hints 解析字符串注解）。
插入位置：模块 docstring 之后、其余 import 之前（PEP 563 要求）。
用法：python research/scripts/add_future_annotations.py [--dry-run]
"""
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
FUTURE = "from __future__ import annotations"


def insert_future(text: str) -> str:
    if FUTURE in text:
        return text
    lines = text.splitlines()
    # 找 docstring 结束位置（首个语句若是字符串字面量）
    idx = 0
    if lines and (lines[0].startswith('"""') or lines[0].startswith("'''")):
        quote = lines[0][:3]
        if len(lines[0]) > 3 and lines[0].endswith(quote) and not lines[0].strip() == quote:
            idx = 1  # 单行 docstring
        else:
            for i in range(1, len(lines)):
                if quote in lines[i]:
                    idx = i + 1
                    break
    # docstring 后可能有空行，插在第一个非空行前
    while idx < len(lines) and not lines[idx].strip():
        idx += 1
    lines.insert(idx, FUTURE)
    return "\n".join(lines) + "\n"


def main():
    dry = "--dry-run" in sys.argv
    n = 0
    for f in sorted((ROOT / "quant_engine").rglob("*.py")):
        if "__pycache__" in f.parts:
            continue
        text = f.read_text(encoding="utf-8")
        if "Optional[" not in text:
            continue
        new = insert_future(text)
        if new != text:
            n += 1
            rel = f.relative_to(ROOT)
            print(f"{'DRY' if dry else 'ADD'} {rel}")
            if not dry:
                f.write_text(new, encoding="utf-8")
    print(f"\n{'would add' if dry else 'added'}: {n} files")


if __name__ == "__main__":
    main()
