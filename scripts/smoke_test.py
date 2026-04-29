import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from modules.analysis import blend_risk_with_sentiment, load_bettafish_signal, risk_score
from modules.market import get_quote
from modules.weekly_ops import check_daily_repo_status


def main():
    data_dir = ROOT / "data" / "daily_stock_analysis"
    betta_dir = ROOT / "data" / "bettafish"
    data_dir.mkdir(parents=True, exist_ok=True)
    betta_dir.mkdir(parents=True, exist_ok=True)

    quote_file = data_dir / "latest_quote.json"
    quote_file.write_text(
        json.dumps({"sh000001": {"name": "上证指数", "price": 3333.33}}, ensure_ascii=False),
        encoding="utf-8",
    )
    betta_file = betta_dir / "latest_report.json"
    betta_file.write_text(json.dumps({"sentiment_score": -0.2}, ensure_ascii=False), encoding="utf-8")

    os.environ["DAILY_STOCK_ANALYSIS_QUOTE_PATH"] = str(quote_file)
    os.environ["QUOTE_SOURCE"] = "daily"

    quote = get_quote("sh000001", source="auto")
    assert str(quote.get("source", "")).startswith("daily_stock_analysis_local")
    assert quote["price"] > 0

    sentiment = load_bettafish_signal(str(betta_file))
    merged = blend_risk_with_sentiment(risk_score(0.2, 1.6), sentiment)
    assert 0 <= merged["score"] <= 100

    repo_status = check_daily_repo_status()
    assert "repo_exists" in repo_status

    print("smoke_test_pass", quote, merged, repo_status["error"])


if __name__ == "__main__":
    main()
