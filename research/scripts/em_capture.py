#!/usr/bin/env python3
"""捕获页面 XHR/Fetch 请求，用于发现接口。"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from cdp import Tab, new_tab, close_tab  # noqa: E402


def capture(url, wait=15, clicks_js=None):
    t = new_tab("about:blank")
    tab = Tab(t["webSocketDebuggerUrl"])
    tab.cmd("Network.enable")
    tab.cmd("Page.enable")
    urls = []
    import threading

    stop = {"v": False}

    def reader():
        tab.ws.settimeout(1.0)
        while not stop["v"]:
            try:
                m = json.loads(tab.ws.recv())
            except Exception:
                continue
            if m.get("method") == "Network.requestWillBeSent":
                r = m["params"]["request"]
                u = r.get("url", "")
                if any(
                    k in u
                    for k in ("api", "jsonp", "json", "list", "query", "get")
                ):
                    urls.append((r.get("method"), u))
    thr = threading.Thread(target=reader, daemon=True)
    thr.start()

    tab.cmd("Page.navigate", {"url": url})
    time.sleep(wait)
    if clicks_js:
        try:
            tab.eval(clicks_js)
        except Exception as e:
            print("click err", e)
        time.sleep(wait)
    stop["v"] = True
    time.sleep(0.5)
    tab.close()
    close_tab(t["id"])
    return urls


if __name__ == "__main__":
    url = sys.argv[1] if len(sys.argv) > 1 else "https://emcreative.eastmoney.com/app_fortune/person/index.html?uid=7924037455558900"
    seen = []
    for m, u in capture(url, wait=14):
        if u not in seen:
            seen.append(u)
    for u in seen:
        print(u[:220])
