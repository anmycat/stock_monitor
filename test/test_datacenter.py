import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quant_engine.data.datacenter import DataCenter

print("Testing DataCenter...")
dc = DataCenter()

print("Getting all realtime quotes...")
df = dc.get_all_realtime_quotes()
print(f"Quotes: {len(df)}")

if not df.empty:
    print(f"Columns: {df.columns.tolist()}")
    overview = dc.get_market_overview()
    print(f"Overview: {overview}")
else:
    print("No data returned")
