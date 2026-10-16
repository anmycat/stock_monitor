#!/usr/bin/env python3
"""
CDP (Chrome DevTools Protocol) helper.
通过本机 Chrome 的 --remote-debugging-port 执行浏览器操作，用于抓取
需要真实浏览器环境（JS 渲染 / WAF / 登录态）的页面。

Usage:
  python3 cdp.py tabs
  python3 cdp.py open <url>            # 新建标签页并等待加载
  python3 cdp.py html <url> [--wait N] # 新开页取渲染后 HTML, 输出到 stdout
  python3 cdp.py text <url> [--wait N] # 新开页取纯文本
  python3 cdp.py eval <url> --js 'JS'  # 新开页执行 JS 并返回 JSON 结果
  python3 cdp.py tabs-eval <idx> --js 'JS'
  python3 cdp.py scroll <url> [--times N] [--wait N]  # 滚动加载后取 HTML
"""
import base64
import json
import os
import sys
import time
import urllib.parse
import urllib.request

import websocket

HOST = "127.0.0.1"
PORT = int(os.environ.get("CDP_PORT", "9222"))
BASE = f"http://{HOST}:{PORT}"

# 绕过本机 HTTP 代理（连 127.0.0.1 必须直连）
_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def _http(path):
    with _opener.open(f"{BASE}{path}", timeout=10) as r:
        return json.loads(r.read().decode())


def list_tabs():
    return [t for t in _http("/json/list") if t.get("type") == "page"]


def new_tab(url=""):
    endpoint = f"/json/new?{urllib.parse.quote(url, safe='')}" if url else "/json/new"
    req = urllib.request.Request(f"{BASE}{endpoint}", method="PUT")
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read().decode())


def close_tab(tid):
    try:
        urllib.request.urlopen(f"{BASE}/json/close/{tid}", timeout=5).read()
    except Exception:
        pass


class Tab:
    def __init__(self, ws_url):
        self.ws = websocket.create_connection(ws_url, timeout=90, suppress_origin=True)
        self._id = 0

    def cmd(self, method, params=None, timeout=90):
        self._id += 1
        self.ws.settimeout(timeout)
        self.ws.send(json.dumps({"id": self._id, "method": method, "params": params or {}}))
        while True:
            msg = json.loads(self.ws.recv())
            if msg.get("id") == self._id:
                if "error" in msg:
                    raise RuntimeError(msg["error"])
                return msg.get("result", {})

    def navigate(self, url, wait=6):
        self.cmd("Page.enable")
        self.cmd("Runtime.enable")
        self.cmd("Page.navigate", {"url": url})
        time.sleep(wait)

    def eval(self, js, timeout=60):
        r = self.cmd(
            "Runtime.evaluate",
            {"expression": js, "returnByValue": True, "awaitPromise": True},
            timeout=timeout,
        )
        if r.get("exceptionDetails"):
            raise RuntimeError(r["exceptionDetails"])
        return r.get("result", {}).get("value")

    def html(self):
        return self.eval("document.documentElement.outerHTML") or ""

    def text(self):
        return self.eval("document.body ? document.body.innerText : ''") or ""

    def scroll(self, times=10, wait=1.5):
        for _ in range(times):
            self.eval("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(wait)

    def close(self):
        try:
            self.ws.close()
        except Exception:
            pass


def open_tab(url, wait=6):
    t = new_tab(url)
    time.sleep(0.6)
    tab = Tab(t["webSocketDebuggerUrl"])
    tab.navigate(url, wait=wait)
    return t["id"], tab


JS_STRIP = """
(() => { const s=document.documentElement.outerHTML; return s; })()
"""


def main():
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        return
    cmd = args[0]

    def opt(name, default=None):
        return args[args.index(name) + 1] if name in args else default

    if cmd == "tabs":
        for i, t in enumerate(list_tabs()):
            print(f"[{i}] {t.get('title','')[:60]} | {t.get('url','')[:100]}")
        return

    if cmd in ("open", "html", "text", "eval", "scroll", "json"):
        url = args[1]
        wait = float(opt("--wait", 8))
        tid, tab = open_tab(url, wait)
        try:
            if cmd == "scroll":
                tab.scroll(int(opt("--times", 12)), float(opt("--pause", 1.5)))
                print(tab.html())
            elif cmd == "text":
                print(tab.text())
            elif cmd == "eval":
                print(json.dumps(tab.eval(args[args.index("--js") + 1]), ensure_ascii=False))
            elif cmd == "json":
                print(tab.eval("document.body.innerText"))
            else:
                print(tab.html())
        finally:
            if cmd != "open":
                close_tab(tid)
            tab.close()
        return

    if cmd == "tabs-eval":
        idx = int(args[1])
        t = list_tabs()[idx]
        tab = Tab(t["webSocketDebuggerUrl"])
        try:
            print(json.dumps(tab.eval(args[args.index("--js") + 1]), ensure_ascii=False))
        finally:
            tab.close()
        return

    print(__doc__)


if __name__ == "__main__":
    main()
