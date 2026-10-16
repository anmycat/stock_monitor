import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quant_engine.data.sources.eastmoney import EastMoneySource

print("Testing EastMoney source...")
df = EastMoneySource.get_all_stocks()
print(f"Result: {len(df)} stocks")

if not df.empty:
    print(f"Columns: {df.columns.tolist()}")
    print(f"Sample:\n{df.head()}")
else:
    print("No data returned")
