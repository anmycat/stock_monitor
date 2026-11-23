#!/usr/bin/env bash
# D78/D79：构建本项目自有 Rust 加速扩展 `qerust`（可选步骤，失败不阻塞部署）
#
#   bash deploy/build_rust_accel.sh
#
# 行为：
#   1. 先按「源码指纹 + 平台 + Python 版本」查产物缓存；命中直接 cp，跳过 cargo
#   2. 无 cargo 且无缓存 → 打印提示后 **exit 0**（服务走纯 Python 回退，行为等价）
#   3. 有 cargo → cargo build，成功后写入缓存（供后续同指纹部署复用）
#   4. 装完做导入自检 + 微基准，失败删产物 + 删缓存（不留半残状态）
# 环境变量：
#   PYTHON_BIN / CARGO_BIN（默认 python3 / cargo）
#   QERUST_CACHE_DIR（默认 $HOME/artifacts/qerust；ECS 为 /root/artifacts/qerust，
#                    在 $REMOTE 之外，rsync --delete 不会误删）
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

PY="${PYTHON_BIN:-python3}"
CARGO="${CARGO_BIN:-cargo}"
SO_DEST="quant_engine/utils/_qerust/qerust.so"

# 平台标识 + 源码指纹（Cargo.toml / Cargo.lock / src/lib.rs / 本脚本 + Python 版本）
PLATFORM="$(uname -m)-$(uname -s)"
FINGERPRINT=$( { sha256sum rust_accel/Cargo.toml rust_accel/Cargo.lock rust_accel/src/lib.rs deploy/build_rust_accel.sh 2>/dev/null || true; "$PY" -V 2>&1; } | sha256sum | cut -c1-20 )
CACHE_DIR="${QERUST_CACHE_DIR:-${HOME}/artifacts/qerust}/$PLATFORM/$FINGERPRINT"
CACHE_SO="$CACHE_DIR/qerust.so"

mkdir -p "$(dirname "$SO_DEST")"

# 自检 + 微基准（失败返回非 0）
self_check() {
    "$PY" - <<'PY'
import os, sys, time
sys.path.insert(0, '.')
from quant_engine.utils import rust_accel
assert rust_accel.available(), '导入失败'
import numpy as np, pandas as pd
rng = np.random.default_rng(7)
closes = np.cumprod(1 + rng.normal(0, 0.01, 240)) * 10
t0 = time.perf_counter()
for _ in range(2000):
    rust_accel.wilder_rsi(pd.Series(closes), 14)
rst = time.perf_counter() - t0
rust_accel.reset_for_test()
os.environ['RUST_ACCEL_ENABLED'] = 'false'
t0 = time.perf_counter()
for _ in range(2000):
    rust_accel.wilder_rsi(pd.Series(closes), 14)
pyt = time.perf_counter() - t0
print(f"  自检通过：backend=rust  rust={rst*1000:.0f}ms  python={pyt*1000:.0f}ms  2000 次 RSI")
PY
}

# ---- 1. 缓存命中：校验产物可用则直接安装，跳过 cargo ----
if [ -f "$CACHE_SO" ]; then
    echo "  命中产物缓存（$PLATFORM/${FINGERPRINT:0:8}）..."
    cp -f "$CACHE_SO" "$SO_DEST"
    if self_check; then
        echo "  ✅ 使用缓存产物（本次未跑 cargo）；源码变更会自动重编"
        exit 0
    fi
    echo "  ⚠️ 缓存产物自检失败，删除缓存与产物后重试编译"
    rm -f "$CACHE_SO" "$SO_DEST"
fi

# ---- 2. 缓存未命中：有 cargo 才编译 ----
if ! command -v "$CARGO" >/dev/null 2>&1; then
    echo "  无 cargo 且无可用缓存 → 跳过（指标走纯 Python 回退，逐位等价）"
    exit 0
fi

# macOS 需要显式允许未定义符号（libpython 由解释器在运行时提供）
EXTRA=()
case "$(uname -s)" in
    Darwin) EXTRA=(-C link-arg=-undefined -C link-arg=dynamic_lookup) ;;
esac

echo "  构建 qerust（$PLATFORM，指纹 ${FINGERPRINT:0:8}）..."
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

cp -f "$LIB" "$SO_DEST"
echo "  已安装：$SO_DEST（$(du -h "$SO_DEST" | cut -f1)）"

if self_check; then
    mkdir -p "$CACHE_DIR"
    cp -f "$SO_DEST" "$CACHE_SO"
    echo "  已缓存到 $CACHE_SO（后续同指纹部署跳过 cargo，约 2m27s → 秒级）"
    echo "  ✅ Rust 加速扩展就绪"
else
    rm -f "$SO_DEST" "$CACHE_SO"
    echo "  ⚠️ 自检失败，已删除产物与缓存，回退纯 Python"
fi
