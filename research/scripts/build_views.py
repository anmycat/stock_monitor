#!/usr/bin/env python3
"""从原始语料抽取结构化「个股观点」记录，供后续项目（回测/打分/知识库）使用。"""
import datetime
import html
import json
import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW = os.path.join(ROOT, "raw")
OUT = os.path.join(ROOT, "data")
os.makedirs(OUT, exist_ok=True)

TAG = re.compile(r"\$([一-龥A-Za-z0-9]{2,10})\(((?:SH|SZ|BJ|HK)?\d{6})\)")
NAME = re.compile(r"([一-龥]{2,8})[（(](?:SH|SZ|BJ|HK)?(\d{6})[)）]")

BULL = ["超预期", "好财报", "景气", "翻倍", "龙头", "兑现", "加速", "放量", "新高", "质量高",
        "干净", "赏心悦目", "看好", "重点", "优质", "真成长", "弹性", "卖铲"]
BEAR = ["不参与", "不感冒", "只看不干", "不感兴趣", "没救", "低基数", "周期", "回撤", "拥挤",
        "腰斩", "膝盖斩", "杀跌", "退潮", "不再", "不买", "没欲望", "难", "瑕疵", "风险"]

CAUTION = ["跟踪", "观察", "等催化", "边走边看", "等待", "只跟踪"]


def clean(s):
    s = re.sub(r"<br\s*/?>", "\n", s or "")
    s = re.sub(r"</p>", "\n", s)
    s = re.sub(r"<[^>]+>", "", s)
    return re.sub(r"\n{2,}", "\n", html.unescape(s)).strip()


def judge(text):
    b = sum(1 for w in BULL if w in text)
    s = sum(1 for w in BEAR if w in text)
    c = sum(1 for w in CAUTION if w in text)
    if b > s:
        return "偏多", b, s, c
    if s > b:
        return "偏空/回避", b, s, c
    return "中性/观察", b, s, c


def rows_xueqiu():
    p = os.path.join(RAW, "xueqiu_jingshui2008.json")
    if not os.path.exists(p):
        return []
    d = json.load(open(p, encoding="utf-8"))
    out = []
    for x in d["posts"]:
        body = x.get("full_text") or x.get("description") or ""
        txt = clean(body)
        dt = datetime.datetime.fromtimestamp(x["created_at"] / 1000).strftime("%Y-%m-%d")
        # 按公司分段：以 "数字、" 开头的条目
        parts = re.split(r"\n?\d{1,2}、", txt)
        for seg in parts[1:]:
            m = re.match(r"\s*([一-龥A-Za-z0-9]{2,10})", seg)
            if not m:
                continue
            name = m.group(1)
            if len(name) < 2:
                continue
            out.append(
                {
                    "source": "雪球",
                    "author": "静水2008",
                    "date": dt,
                    "stock": name,
                    "code": "",
                    "stance": judge(seg)[0],
                    "bull_hits": judge(seg)[1],
                    "bear_hits": judge(seg)[2],
                    "caution_hits": judge(seg)[3],
                    "text": seg[:600],
                    "url": f"https://xueqiu.com/{x['user_id']}/{x['id']}",
                }
            )
    return out


def rows_zhihu():
    p = os.path.join(RAW, "zhihu_jingshui2008.json")
    if not os.path.exists(p):
        return []
    d = json.load(open(p, encoding="utf-8"))
    out = []
    for x in d["articles"]:
        txt = clean(x.get("content") or "")
        dt = datetime.datetime.fromtimestamp(x.get("created") or 0).strftime("%Y-%m-%d")
        url = f"https://zhuanlan.zhihu.com/p/{x.get('id','')}"
        parts = re.split(r"\n?\d{1,2}、", txt)
        for seg in parts[1:]:
            m = re.match(r"\s*([一-龥A-Za-z0-9]{2,10})", seg)
            if not m or len(m.group(1)) < 2:
                continue
            out.append(
                {
                    "source": "知乎文章",
                    "author": "静水2008",
                    "date": dt,
                    "stock": m.group(1),
                    "code": "",
                    "stance": judge(seg)[0],
                    "bull_hits": judge(seg)[1],
                    "bear_hits": judge(seg)[2],
                    "caution_hits": judge(seg)[3],
                    "text": seg[:600],
                    "url": url,
                }
            )
    return out


def rows_em():
    p = os.path.join(RAW, "em_duokong.json")
    if not os.path.exists(p):
        return []
    d = json.load(open(p, encoding="utf-8"))
    seen, out = set(), []
    for _, lst in d["items"].items():
        for x in lst:
            pid = str(x.get("post_id"))
            if pid in seen:
                continue
            seen.add(pid)
            body = clean((x.get("post_title") or "") + "\n" + (x.get("post_content") or ""))
            if not body:
                continue
            t = x.get("post_publish_time") or ""
            names = [(m.group(1), m.group(2)) for m in TAG.finditer(body)]
            if not names:
                names = [("", "")]
            for nm, cd in names:
                out.append(
                    {
                        "source": "东方财富",
                        "author": "多空博弈社",
                        "datetime": t[:16],
                        "date": t[:10],
                        "stock": nm,
                        "code": cd,
                        "stance": judge(body)[0],
                        "bull_hits": judge(body)[1],
                        "bear_hits": judge(body)[2],
                        "caution_hits": judge(body)[3],
                        "text": body[:300],
                        "url": "https://emcreative.eastmoney.com/app_fortune/person/index.html?uid=7924037455558900",
                    }
                )
    return out


def main():
    allrows = rows_xueqiu() + rows_zhihu() + rows_em()
    with open(os.path.join(OUT, "stock_views.json"), "w", encoding="utf-8") as f:
        json.dump(allrows, f, ensure_ascii=False, indent=1)

    import csv
    cols = ["source", "author", "date", "datetime", "stock", "code", "stance",
            "bull_hits", "bear_hits", "caution_hits", "text", "url"]
    with open(os.path.join(OUT, "stock_views.csv"), "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for r in allrows:
            w.writerow(r)

    print(f"总记录 {len(allrows)}")
    for src in ("雪球", "知乎文章", "东方财富"):
        n = sum(1 for r in allrows if r["source"] == src)
        print(f"  {src}: {n}")
    print(f"-> {OUT}/stock_views.json / .csv")


if __name__ == "__main__":
    main()
