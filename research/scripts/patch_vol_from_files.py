# -*- coding: utf-8 -*-
"""从 neodata 落盘文件解析 volume/amplitude/turnover，合并进 raw/market_full.json
覆盖 9月/10月/8月中旬 三个窗口的补拉数据。
"""
import json
import os

TR = "/Users/hr.wh/.workbuddy/projects/Users-hr.wh-WorkBuddy-2026-10-13-00-21-29/9ac5c217-5538-4516-87a8-1ccbb7571601/tool-results"
FILES = [
    "chatcmpl-tool-9c130daa6b0c79a3.txt",          # 9月 批次1 (000011~600076)
    "chatcmpl-tool-81a3c7d5b14b24f3.txt",          # 9月 批次2 (600162~603823)
    "chatcmpl-tool-b9d7ebf265883981-render.txt",   # 10月 批次1
    "chatcmpl-tool-9b22125ba83a58d1-render.txt",   # 10月 批次2
    "chatcmpl-tool-b2ad4a03388b1e17-render.txt",   # 8月中旬 批次1
    "chatcmpl-tool-8f310550628f7ed0-render.txt",   # 8月中旬 批次2
    "chatcmpl-tool-96b7bb871d87c4da-render.txt",   # 8/19-8/31 批次1
    "chatcmpl-tool-b270da1c42c5a5d5-render.txt",   # 8/19-8/31 批次2
    "chatcmpl-tool-9f9e7ed6a9e50d51-render.txt",   # 8/20-8/21 批次2
]

# 8/20-8/21 批次1（trace ba52f675）未落盘，手动内联
EXTRA = {
    "000011.SZ": {"2026-08-20": (10158900, 5.35, 1.93), "2026-08-21": (6305900, 3.17, 1.20)},
    "000510.SZ": {"2026-08-20": (52100900, 9.06, 8.59), "2026-08-21": (35241600, 6.46, 5.81)},
    "001258.SZ": {"2026-08-20": (118401700, 9.48, 12.69), "2026-08-21": (102267900, 7.80, 10.96)},
    "001330.SZ": {"2026-08-20": (56726300, 5.59, 4.88), "2026-08-21": (36048300, 3.33, 3.10)},
    "002081.SZ": {"2026-08-20": (523699800, 10.82, 19.81), "2026-08-21": (408993800, 7.75, 15.47)},
    "002141.SZ": {"2026-08-20": (143645600, 10.37, 13.91), "2026-08-21": (144751600, 7.18, 14.01)},
    "002165.SZ": {"2026-08-20": (178280100, 12.55, 24.50), "2026-08-21": (166685800, 11.42, 22.91)},
    "600076.SH": {"2026-08-20": (17437500, 3.27, 1.30), "2026-08-21": (11675400, 3.22, 0.87)},
}


def _num(v):
    """'-' / '未开盘' / 占位行 → None；否则转 float"""
    if v is None:
        return None
    s = str(v).strip()
    if s in ("-", "未开盘", "") or "省略" in s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def load_all():
    """返回 {code: {date: {"vol":..,"amp":..,"turn":..,"o":..,"c":..}}}"""
    out = {}
    for fn in FILES:
        raw = open(os.path.join(TR, fn), encoding="utf-8").read()
        data = json.loads(raw)
        if "content" in data and "result" not in data:
            data = json.loads(data["content"])  # render 文件双层包裹
        for blk in data.get("result", []):
            code = blk["code"]
            slot = out.setdefault(code, {})
            for k in blk.get("kline_list", []):
                day = k.get("trade_date", "")
                if len(day) != 10 or "~" in day:
                    continue  # 跳过占位摘要行
                vol = _num(k.get("volume"))
                amp = _num(k.get("amplitude"))
                turn = _num(k.get("turnover_rate"))
                op = _num(k.get("open"))
                cl = _num(k.get("close"))
                if vol is None and amp is None:
                    continue
                rec = slot.setdefault(day, {})
                if vol is not None:
                    rec["vol"] = vol
                if amp is not None:
                    rec["amp"] = amp
                if turn is not None:
                    rec["turn"] = turn
                if op is not None:
                    rec["o"] = op
                if cl is not None:
                    rec["c"] = cl
    return out


def main():
    patch = load_all()
    # 合入内联的 EXTRA
    for code, days in EXTRA.items():
        slot = patch.setdefault(code, {})
        for day, (vol, amp, turn) in days.items():
            rec = slot.setdefault(day, {})
            rec.setdefault("vol", vol); rec.setdefault("amp", amp); rec.setdefault("turn", turn)

    n_days = sum(len(v) for v in patch.values())
    print(f"解析到 {len(patch)} 只票 / {n_days} 个交易日")

    d = json.load(open("raw/market_full.json"))
    added = 0
    for code, days in patch.items():
        slot = d["daily"].setdefault(code, {"name": "", "d": {}})
        for day, rec in days.items():
            tgt = slot["d"].setdefault(day, {})
            for k, v in rec.items():
                if k == "vol":
                    # vol 只补缺失/0，不覆盖
                    if not tgt.get(k):
                        tgt[k] = v; added += 1
                elif not tgt.get(k):
                    tgt[k] = v
    d["_meta"]["vol_patch2"] = "2026-10-14 从落盘文件自动合并 9月/10月/8月中旬 volume/amplitude/turnover"
    json.dump(d, open("raw/market_full.json", "w"), ensure_ascii=False)

    zero_vol = sum(1 for blk in d["daily"].values() for rec in blk["d"].values() if not rec.get("vol"))
    zero_amp = sum(1 for blk in d["daily"].values() for rec in blk["d"].values() if not rec.get("amp"))
    print(f"新增 vol {added} 条；剩余 vol=0 天数 {zero_vol}；amp=0 天数 {zero_amp}")
    # 抽验
    print("抽验 600162 2026-09-30:", d["daily"]["600162.SH"]["d"].get("2026-09-30"))
    print("抽验 002165 2026-09-16:", d["daily"]["002165.SZ"]["d"].get("2026-09-16"))


if __name__ == "__main__":
    main()
