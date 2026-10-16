#!/usr/bin/env python3
import json
import os

base = "/Users/hr.wh/WorkBuddy/2026-10-13-00-21-29"
man = json.load(open(os.path.join(base, "raw/ks_video_covers/frames_manifest.json"), encoding="utf-8"))

# 分配：A=JKn0ajlI(5)+Jdg4jr6z(2)=7, B=nZTKyjXA(4)+JgZ1IFSc(4)=8, C=nmP9R9Lt(4)=4
plan = {
    "A": ["JKn0ajlI", "Jdg4jr6z"],
    "B": ["nZTKyjXA", "JgZ1IFSc"],
    "C": ["nmP9R9Lt"],
}
for k, codes in plan.items():
    out = []
    for code in codes:
        for r in man:
            if r["code"] == code:
                out.append({"video_code": code, "hint": r["hint"], "images": r["images"]})
    p = os.path.join(base, f"data/ks_video_batch_{k}.json")
    json.dump(out, open(p, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    n = sum(len(x["images"]) for x in out)
    print(f"batch {k}: {len(out)} videos, {n} images -> {p}")
