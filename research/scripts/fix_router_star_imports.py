"""P3 收尾：将 12 个域 router 的 `from ..schemas import *` 替换为显式导入。

星号导入触发 pyflakes F403/F405（384 项），违反 prd/15 §3.2 每阶段"ruff F/E9=0"验收。
方法：AST 解析各 router 实际引用的标识符 ∩ schemas.py 顶层定义，生成最小显式导入集。
用法：python research/scripts/fix_router_star_imports.py [--dry-run]
"""
import ast
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
ROUTERS_DIR = ROOT / "quant_engine" / "api" / "routers"
SCHEMAS = ROOT / "quant_engine" / "api" / "schemas.py"
STAR_LINE = "from ..schemas import *  # noqa: F401,F403——域响应模型"

# schemas.py 之外的合法内置/常用名（typing 等），不与 schemas 求交时误伤
schema_tree = ast.parse(SCHEMAS.read_text(encoding="utf-8"))
schema_names = set()
for node in schema_tree.body:
    if isinstance(node, ast.ClassDef):
        schema_names.add(node.name)
    elif isinstance(node, ast.Assign):
        for target in node.targets:
            if isinstance(target, ast.Name):
                schema_names.add(target.id)
    elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
        schema_names.add(node.target.id)


def used_names(source: str) -> set:
    tree = ast.parse(source)
    return {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}


def build_import_line(names):
    if len(names) <= 3:
        return "from ..schemas import " + ", ".join(names)
    body = "".join(f"    {n},\n" for n in names)
    return f"from ..schemas import (\n{body})"


def main():
    dry = "--dry-run" in sys.argv
    changed = 0
    for f in sorted(ROUTERS_DIR.glob("*.py")):
        if f.name == "__init__.py":
            continue
        src = f.read_text(encoding="utf-8")
        if STAR_LINE not in src:
            print(f"SKIP {f.name}: 无星号导入行")
            continue
        needed = sorted(
            n for n in (used_names(src) & schema_names)
            # 下划线常量（_CODE_RE/_DATE_RE 等）各 router 均已从 _shared 导入且定义等价，
            # 不属于 schemas 导出面；星号导入时代的 shadow 无行为差异，显式化时不再带入
            if not n.startswith("_")
        )
        if not needed:
            print(f"WARN {f.name}: 未引用任何 schemas 名称，直接删除星号行")
            new = src.replace(STAR_LINE + "\n", "")
        else:
            new = src.replace(STAR_LINE, build_import_line(needed))
        if new != src:
            changed += 1
            print(f"{'DRY' if dry else 'FIX'} {f.name}: {needed}")
            if not dry:
                f.write_text(new, encoding="utf-8")
    print(f"\n{'would change' if dry else 'changed'}: {changed} files")


if __name__ == "__main__":
    main()
