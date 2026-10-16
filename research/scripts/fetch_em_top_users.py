#!/usr/bin/env python3
"""抓取东方财富 2025 年度榜单头部用户内容（股吧内容牛人10 + 实盘牛人10，共19个可解析uid）。

数据源：gbapi.eastmoney.com/userpostlist/api/UserPost/CenterList（与 fetch_em.py 同参数集）
产出：raw/em_top_users/{用户名}.json + raw/em_top_users/_all.json
"""
import json
import os
import re
import time

import requests

UID_MAP = {
    "7731112319297770": "花丛冷眼",
    "4571315979945492": "风浪越大越精神",
    "3738113772867614": "滇南王",
    "2457112361206618": "Mystock948",
    "6835047426969636": "新马门溪财经",
    "2751376389404378": "只爱男大学生",
    "1192094488938384": "静神00988",
    "5744094282152990": "语润梧桐",
    "6601086923558744": "西南小游资",
    "6826366376700514": "顶上抄底",
    "5949077238176390": "炒股养你佛山炒家",
    "1011045311805558": "文辉永续",
    "7109212523843968": "五方财神降临",
    "4711316263152898": "吃完饭再说吧",
    "7181615225720084": "小柚子丶",
    "5720094771159866": "三三彡",
    "2012094520785316": "wxm1988meng",
    "1655275676187720": "割不动的韭菜9527",
    "9320094089256080": "魅力皇a玛",
}

# 榜单归类（来自东财2025年度牛人评选公告）
CATEGORY = {
    "花丛冷眼": "内容牛人", "风浪越大越精神": "内容牛人", "滇南王": "内容牛人",
    "Mystock948": "内容牛人", "新马门溪财经": "内容牛人", "只爱男大学生": "内容牛人",
    "静神00988": "内容牛人", "语润梧桐": "内容牛人", "西南小游资": "内容牛人",
    "顶上抄底": "内容牛人",
    "炒股养你佛山炒家": "实盘牛人", "文辉永续": "实盘牛人", "五方财神降临": "实盘牛人",
    "吃完饭再说吧": "实盘牛人", "小柚子丶": "实盘牛人", "三三彡": "实盘牛人",
    "wxm1988meng": "实盘牛人", "割不动的韭菜9527": "实盘牛人", "魅力皇a玛": "实盘牛人",
}

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
API = "https://gbapi.eastmoney.com/userpostlist/api/UserPost/CenterList"
OUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "raw", "em_top_users")
TYPES = [1, 2, 3]  # 动态 / 帖子 / 长文


def fetch(uid, ptype, page, ps=20):
    params = {
        "uid": uid, "onlyYt": 0, "callback_param": "", "ctoken": "", "utoken": "",
        "deviceid": "", "version": "9001", "product": "Guba", "plat": "Web",
        "gtoken": "", "type": ptype, "filterType": 0, "noinlist": 0,
        "p": page, "ps": ps, "callback": "cb",
    }
    try:
        r = requests.get(API, params=params,
                         headers={"User-Agent": UA, "Referer": "https://emcreative.eastmoney.com/"},
                         timeout=20)
        m = re.search(r"^cb\((.*)\)$", r.text.strip(), re.S)
        if not m:
            return None
        return json.loads(m.group(1))
    except Exception as e:
        print(f"    fetch error: {e}")
        return None


def fetch_user(uid, name):
    items = {}
    for ptype in TYPES:
        got, page, seen = [], 1, set()
        while page <= 30:
            d = fetch(uid, ptype, page)
            if not d:
                break
            re_ = d.get("re") or []
            if not re_:
                break
            new = [x for x in re_ if (x.get("post_id") or id(x)) not in seen]
            for x in re_:
                seen.add(x.get("post_id") or id(x))
            got.extend(new)
            if not new:
                break
            page += 1
            time.sleep(0.35)
        if got:
            items[str(ptype)] = got
    return items


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    meta = {}
    for uid, name in UID_MAP.items():
        out_path = os.path.join(OUT_DIR, f"{name}.json")
        if os.path.exists(out_path):
            print(f"[{name}] 已存在，跳过", flush=True)
            try:
                old = json.load(open(out_path, encoding="utf-8"))
                meta[name] = {"uid": uid, "name": name, "category": CATEGORY.get(name, ""),
                              "counts": {k: len(v) for k, v in old.get("items", {}).items()}}
            except Exception:
                pass
            continue
        print(f"[{name}] uid={uid} ...", flush=True)
        items = fetch_user(uid, name)
        total = sum(len(v) for v in items.values())
        print(f"  -> {total} 条", flush=True)
        rec = {"uid": uid, "name": name, "category": CATEGORY.get(name, ""), "items": items}
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(rec, f, ensure_ascii=False)
        meta[name] = {"uid": uid, "name": name, "category": CATEGORY.get(name, ""),
                      "counts": {k: len(v) for k, v in items.items()}}
        del items, rec  # 及时释放内存
        time.sleep(0.4)

    with open(os.path.join(OUT_DIR, "_all.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False)

    print("\n===== 汇总 =====")
    grand = 0
    for name, m in meta.items():
        t = sum(m.get("counts", {}).values())
        grand += t
        print(f"  {m['category']:5} {name:16} {t:4} 条")
    print(f"总计 {grand} 条，{len(meta)} 位用户 -> {OUT_DIR}/_all.json")


if __name__ == "__main__":
    main()
