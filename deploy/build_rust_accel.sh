#!/usr/bin/env bash
# D78：构建本项目自有 Rust 加速扩展 `qerust`（可选步骤，失败不阻塞部署）
#
#   bash deploy/build_rust_accel.sh
#
# 行为：
#   1. 无 cargo → 打印提示后 **exit 0**（服务继续走纯 Python 回退，行为等价）
#   2. 有 cargo → 编译 rust_accel/ 并拷到 quant_engine/utils/_qerust/qerust.so
#   3. 构建完做一次导入自检（顺带打印 backend 与耗时对比）
# 环境变量：PYTHON_BIN（默认 python3）、CARGO_BIN（默认 cargo）
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

PY="${PYTHON_BIN:-python3}"
CARGO="${CARGO_BIN:-cargo}"

if ! command -v "$CARGO" >/dev/null 2>&1; then
    echo "  无 cargo，跳过 Rust 加速扩展构建（指标将走纯 Python 回退，逐位等价）"
    exit 0
fi

# macOS 需要显式允许未定义符号（libpython 由解释器在运行时提供）
EXTRA=()
case "$(uname -s)" in
    Darwin) EXTRA=(-C link-arg=-undefined -C link-arg=dynamic_lookup) ;;
esac

echo "  构建 qerust（$(uname -m) $(uname -s)）..."
if ! RUSTFLAGS="${EXTRA[*]}" "$CARGO" build --release --features extension-module \
        --manifest-path rust_accel/Cargo.toml 2>&1 | tail -3; then
    echo "  ⚠️ qerust 构建失败，服务将回退纯 Python（不阻塞部署）"
    exit 0
fi

LIB="rust_accel/target/release/libqerust.dylib"
[ -f "$LIB" ] || LIB="rust_accel/target/release/libqerust.so"
if [ ! -f "$LIB" ]; then
    echo "  ⚠️ 未找到构建产物（$LIB），回退纯 Python"
    exit 0
fi

mkdir -p quant_engine/utils/_qerust
cp -f "$LIB" quant_engine/utils/_qerust/qerust.so
echo "  已安装：quant_engine/utils/_qerust/qerust.so（$(du -h quant_engine/utils/_qerust/qerust.so | cut -f1)）"

# 导入自检 + 微基准（失败则删产物，确保不留半残状态）
if "$PY" - <<'PY'
import sys, time
sys.path.insert(0, '.')
from quant_engine.utils import rust_accel
assert rust_accel.available(), '导入失败'
import numpy as np
rng = np.random.default_rng(7)
closes = np.cumprod(1 + rng.normal(0, 0.01, 240)) * 10
t0 = time.perf_counter()
for _ in range(2000):
    rust_accel.wilder_rsi(__import__('pandas').Series(closes), 14)
rst = time.perf_counter() - t0
rust_accel.reset_for_test()
import os
os.environ['RUST_ACCEL_ENABLED'] = 'false'
t0 = time.perf_counter()
for _ in range(2000):
    rust_accel.wilder_rsi(__import__('pandas').Series(closes), 14)
pyt = time.perf_counter() - t0
print(f"  自检通过：backend={ 'rust' }  rust={rst*1000:.0f}ms  python={pyt*1000:.0f}ms  2000 次 RSI")
PY
then
    echo "  ✅ Rust 加速扩展就绪"
else
    rm -f quant_engine/utils/_qerust/qerust.so
    echo "  ⚠️ 自检失败，已删除产物，回退纯 Python"
fi
