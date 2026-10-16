#!/usr/bin/env python3
"""抓取东方财富创作平台 用户(多空博弈社 uid=7924037455558900)全部内容。"""
import json
import os
import re
import time

import requests

UID = "7924037455558900"
OUT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "raw", "em_duokong.json"
)
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
API = "https://gbapi.eastmoney.com/userpostlist/api/UserPost/CenterList"

TYPES = {0: "动态", 1: "动态", 2: "帖子", 3: "长文", 4: "评论", 5: "组合", 6: "其他"}


def fetch(ptype, page, ps=30):
    params = {
        "uid": UID,
        "onlyYt": 0,
        "callback_param": "",
        "ctoken": "",
        "utoken": "",
        "deviceid": "",
        "version": "9001",
        "product": "Guba",
        "plat": "Web",
        "gtoken": "",
        "type": ptype,
        "filterType": 0,
        "noinlist": 0,
        "p": page,
        "ps": ps,
        "callback": "cb",
    }
    r = requests.get(API, params=params, headers={"User-Agent": UA, "Referer": "https://emcreative.eastmoney.com/"}, timeout=20)
    m = re.search(r"^cb\((.*)\)$", r.text.strip(), re.S)
    if not m:
        return None
    return json.loads(m.group(1))


def main():
    result = {"uid": UID, "name": "多空博弈社", "items": {}}
    for ptype, label in TYPES.items():
        got, page, seen = [], 1, set()
        while page <= 200:
            d = fetch(ptype, page, ps=20)
            if not d:
                print(f"type={ptype}({label}) p={page}: parse fail", flush=True)
                break
            re_ = d.get("re") or []
            if not re_:
                print(f"type={ptype}({label}) p={page}: empty, stop", flush=True)
                break
            new = [x for x in re_ if (x.get("post_id") or id(x)) not in seen]
            for x in re_:
                seen.add(x.get("post_id") or id(x))
            got.extend(new)
            print(f"type={ptype}({label}) p={page}: +{len(new)} (累计 {len(got)})", flush=True)
            if not new:
                break
            page += 1
            time.sleep(0.5)
        if got:
            result["items"][f"{ptype}_{label}"] = got
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False)
    total = sum(len(v) for v in result["items"].values())
    print(f"\n合计 {total} 条 -> {OUT}")


if __name__ == "__main__":
    main()
