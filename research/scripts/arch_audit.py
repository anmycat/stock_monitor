"""架构审计（prd/15 §4）：五层依赖宪法 + services 六子包 DAG + 循环依赖。

P2 建立时的现场审计脚本，P3 存档复用。解析 quant_engine 全部内部 import
（模块级与函数内延迟导入分开统计），机器可检：
  1. Tarjan SCC 循环检测（合并两级 import 的强连通视图）
  2. 分层违规：L0 api → L1 services → L2 strategies/backtest → L3 data/models → L4 utils
     低层 import 高层即违规；config 为基础设施不参与分层；入口模块不参与
  3. services 子包 DAG 宪法（prd/15 §2.3）：
     - 叶族 monitors/market/risk/notifications：零子包依赖
     - signals → {monitors, market, risk, notifications}
     - ai → {monitors, market}
     - code_guard/portfolio 顶层自由
用法：python research/scripts/arch_audit.py [--verbose]
退出码：0 = 通过；1 = 有违规
"""
import ast
import pathlib
import sys
from collections import defaultdict

ROOT = pathlib.Path(__file__).resolve().parents[2]
PKG = ROOT / "quant_engine"

# 分层（数字越小越高层；低层依赖高层 = 违规）
LAYERS = {
    "quant_engine.api": 0,
    "quant_engine.services": 1,
    "quant_engine.strategies": 2, "quant_engine.backtest": 2,
    "quant_engine.data": 3, "quant_engine.models": 3,
    "quant_engine.utils": 4,
}
ENTRY_MODULES = {"quant_engine.main", "quant_engine.worker", "quant_engine.beat", "quant_engine.cli"}
INFRA = {"quant_engine.config"}

SERVICE_SUBPKGS = {"monitors", "market", "risk", "signals", "notifications", "ai"}
LEAF_SUBPKGS = {"monitors", "market", "risk", "notifications"}
DAG_CONSTITUTION = {
    "signals": {"monitors", "market", "risk", "notifications"},
    "ai": {"monitors", "market"},
}


def module_name(path: pathlib.Path) -> str:
    rel = path.relative_to(ROOT).with_suffix("")
    parts = list(rel.parts)
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts)


def resolve_from(current_mod: str, node: ast.ImportFrom, is_init: bool = False) -> str:
    """解析 ImportFrom（含相对导入）为绝对模块名。

    基准包规则：普通模块 a.b.c 的基准是 a.b；__init__.py（模块名即包 a.b）的基准是 a.b 本身。
    之后每级 level 向上退 1 个组件。
    """
    if node.level:
        parts = current_mod.split(".")
        base = parts if is_init else parts[:-1]
        base = base[: len(base) - (node.level - 1)]
        if not base:
            return ""
        mod = ".".join(base)
        if node.module:
            mod = mod + "." + node.module
        return mod
    return node.module or ""


def collect_imports():
    """返回 {module: set(imported_internal_modules)}，含模块级与函数内。"""
    graph = defaultdict(set)
    lazy = defaultdict(set)
    for f in sorted(PKG.rglob("*.py")):
        if ".venv" in f.parts or "__pycache__" in f.parts:
            continue
        mod = module_name(f)
        is_init = f.name == "__init__.py"
        try:
            tree = ast.parse(f.read_text(encoding="utf-8"), filename=str(f))
        except SyntaxError as e:
            print(f"PARSE-ERROR {f}: {e}")
            continue
        # 模块级 import = tree.body 直接出现的 Import/ImportFrom（不下钻函数/类/If）
        top_level_ids = {
            id(n) for n in tree.body
            if isinstance(n, (ast.Import, ast.ImportFrom))
        }
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for a in node.names:
                    tgt = a.name
                    if tgt.startswith("quant_engine"):
                        graph[mod].add(tgt)
            elif isinstance(node, ast.ImportFrom):
                tgt = resolve_from(mod, node, is_init)
                if tgt.startswith("quant_engine"):
                    if id(node) in top_level_ids:
                        graph[mod].add(tgt)
                    else:
                        lazy[mod].add(tgt)
    return graph, lazy


def tarjan_scc(graph):
    """迭代 Tarjan；graph: {node: set(neighbors)}"""
    index = {}
    low = {}
    on_stack = set()
    stack = []
    sccs = []
    counter = [0]
    nodes = set(graph)
    for nbrs in graph.values():
        nodes |= nbrs
    for root in sorted(nodes):
        if root in index:
            continue
        work = [(root, iter(sorted(graph.get(root, ()))))]
        index[root] = low[root] = counter[0]
        counter[0] += 1
        stack.append(root)
        on_stack.add(root)
        while work:
            v, it = work[-1]
            advanced = False
            for w in it:
                if w not in index:
                    index[w] = low[w] = counter[0]
                    counter[0] += 1
                    stack.append(w)
                    on_stack.add(w)
                    work.append((w, iter(sorted(graph.get(w, ())))))
                    advanced = True
                    break
                elif w in on_stack:
                    low[v] = min(low[v], index[w])
            if advanced:
                continue
            work.pop()
            if work:
                parent = work[-1][0]
                low[parent] = min(low[parent], low[v])
            if low[v] == index[v]:
                scc = []
                while True:
                    w = stack.pop()
                    on_stack.discard(w)
                    scc.append(w)
                    if w == v:
                        break
                if len(scc) > 1 or v in graph.get(v, ()):
                    sccs.append(scc)
    return sccs


def layer_of(mod: str):
    for prefix, lv in LAYERS.items():
        if mod == prefix or mod.startswith(prefix + "."):
            return lv
    return None  # 入口/基础设施/其他


def subpkg_of(mod: str):
    """services 子包名；顶层 code_guard/portfolio 返回 'TOP:<name>'。

    归一化：'TOP:notifications'（__init__ 自身）与 'notifications'（其子模块）视为同子包。
    """
    parts = mod.split(".")
    if len(parts) < 3 or parts[:2] != ["quant_engine", "services"]:
        return None
    if len(parts) == 3:
        return parts[2]
    return parts[2]


def main():
    verbose = "--verbose" in sys.argv
    graph, lazy = collect_imports()
    merged = defaultdict(set)
    for m, s in graph.items():
        merged[m] |= s
    for m, s in lazy.items():
        merged[m] |= s

    problems = []

    # 1. 循环依赖（模块级优先报告，延迟导入单独标注）
    sccs = tarjan_scc(graph)
    lazy_sccs = tarjan_scc(merged)
    module_level_cycles = [sorted(s) for s in sccs]
    all_cycles = [sorted(s) for s in lazy_sccs]
    if module_level_cycles:
        problems.append(f"模块级循环依赖 {len(module_level_cycles)} 处: {module_level_cycles}")

    # 2. 分层违规（只看模块级 + 延迟，均计入）
    for m, targets in sorted(merged.items()):
        lm = layer_of(m)
        if lm is None:
            continue  # 入口/基础设施不参与
        for t in sorted(targets):
            lt = layer_of(t)
            if lt is None:
                continue
            if lm > lt:
                where = "模块级" if t in graph.get(m, ()) else "延迟"
                problems.append(f"分层违规[{where}] {m}(L{lm}) -> {t}(L{lt})")

    # 3. services 子包 DAG 宪法（模块级 + 延迟都算边）
    cross = []
    for m, targets in sorted(merged.items()):
        sm = subpkg_of(m)
        if sm is None:
            continue
        for t in sorted(targets):
            st = subpkg_of(t)
            if st is None or st == sm:
                continue
            cross.append((sm, st, m, t, "模块级" if t in graph.get(m, ()) else "延迟"))
    leaf_edges = [c for c in cross if c[0] in LEAF_SUBPKGS]
    if leaf_edges:
        problems.append(f"叶族子包依赖违规 {len(leaf_edges)} 处: "
                        + "; ".join(f"{c[0]}->{c[1]} ({c[2]} [{c[4]}])" for c in leaf_edges))
    illegal = [c for c in cross
               if c[0] in DAG_CONSTITUTION and c[1] not in DAG_CONSTITUTION[c[0]]]
    if illegal:
        problems.append("宪法外跨族边: " + "; ".join(f"{c[0]}->{c[1]} ({c[2]} [{c[4]}])" for c in illegal))

    # 报告
    n_files = len(list(PKG.rglob("*.py")))
    n_edges = sum(len(s) for s in merged.values())
    n_lazy = sum(len(s) for s in lazy.values())
    print(f"审计范围: {n_files} 模块 / {n_edges} 内部依赖边（其中延迟导入 {n_lazy}）")
    print(f"跨子包边（services 内部）: {len(cross)}")
    if verbose:
        for sm, st, m, t, kind in cross:
            print(f"  {sm:14s} -> {st:14s}  {m} [{kind}]")
        print(f"循环（含延迟视图）: {len(all_cycles)} 组（模块级视图 {len(module_level_cycles)} 组）")
        if all_cycles:
            for c in all_cycles:
                print(f"  SCC: {' -> '.join(c)}")
    if problems:
        print(f"\n❌ {len(problems)} 项违规:")
        for p in problems:
            print(f"  - {p}")
        return 1
    print("\n✅ 架构审计通过：零模块级循环 / 分层宪法合规 / services DAG 宪法合规")
    return 0


if __name__ == "__main__":
    sys.exit(main())
