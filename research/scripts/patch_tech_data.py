#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""补录两次未落盘的 technical 调用结果（002165.SZ 红宝丽 8/31、601086.SH 国芳集团 9/14-9/16）。"""
import json
import os

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PATH = os.path.join(BASE, "raw/market_full.json")

PATCH = {
    "002165.SZ": {
        "name": "红宝丽",
        "d": {
            "2026-08-31": {"close": 8.59, "ma5": 7.964, "ma20": 7.268, "rsi6": 76.043,
                            "kdj_j": 85.923, "bias_ma20": 18.197, "vr": 179.197,
                            "chip_profit_rate": 72.89, "chip_avg_cost": 7.91},
        },
    },
    "000011.SZ": {
        "name": "深物业A",
        "d": {
            "2026-10-08": {"close": 12.12, "ma5": 10.962, "ma20": 9.454, "rsi6": 83.675,
                            "kdj_j": 90.24, "bias_ma20": 28.206, "vr": 88.616,
                            "chip_profit_rate": 76.78, "chip_avg_cost": 9.51},
        },
    },
    "001330.SZ": {
        "name": "博纳影业",
        "d": {
            "2026-10-08": {"close": 6.34, "ma5": 6.338, "ma20": 5.868, "rsi6": 64.202,
                            "kdj_j": 81.668, "bias_ma20": 8.053, "vr": 135.497,
                            "chip_profit_rate": 60.7, "chip_avg_cost": 6.21},
        },
    },
    "600664.SH": {
        "name": "哈药股份",
        "d": {
            "2026-10-08": {"close": 7.24, "ma5": 7.45, "ma20": 7.708, "rsi6": 38.096,
                            "kdj_j": -2.214, "bias_ma20": -6.066, "vr": 79.583,
                            "chip_profit_rate": 12.35, "chip_avg_cost": 7.57},
        },
    },
    "601086.SH": {
        "name": "国芳集团",
        "d": {
            "2026-09-14": {"close": 17.99, "ma5": 15.856, "ma20": 10.881, "rsi6": 98.334,
                            "kdj_j": 103.141, "bias_ma20": 65.334, "vr": 554.424,
                            "chip_profit_rate": 100.0, "chip_avg_cost": 14.43},
            "2026-09-15": {"close": 16.19, "ma5": 16.104, "ma20": 11.293, "rsi6": 68.939,
                            "kdj_j": 82.994, "bias_ma20": 43.369, "vr": 398.883,
                            "chip_profit_rate": 77.25, "chip_avg_cost": 14.55},
            "2026-09-16": {"close": 14.57, "ma5": 16.042, "ma20": 11.64, "rsi6": 52.115,
                            "kdj_j": 55.072, "bias_ma20": 25.177, "vr": 316.145,
                            "chip_profit_rate": 48.33, "chip_avg_cost": 14.61},
        },
    },
}


def main():
    m = json.load(open(PATH, encoding="utf-8"))
    for code, payload in PATCH.items():
        tech = m["tech"].setdefault(code, {"name": payload["name"], "d": {}})
        tech["name"] = payload["name"]
        tech["d"].update(payload["d"])
    m["_meta"]["patched"] = ["002165.SZ@2026-08-31", "601086.SH@2026-09-14..16",
                             "600664.SH@2026-10-08", "000011.SZ@2026-10-08",
                             "001330.SZ@2026-10-08"]
    with open(PATH, "w", encoding="utf-8") as f:
        json.dump(m, f, ensure_ascii=False)
    print("补录完成。tech 股票数:", len(m["tech"]))
    for c in ("002165.SZ", "601086.SH", "600664.SH", "000011.SZ", "001330.SZ"):
        print(f"  {c} {m['tech'][c]['name']}: {sorted(m['tech'][c]['d'])}")


if __name__ == "__main__":
    main()
