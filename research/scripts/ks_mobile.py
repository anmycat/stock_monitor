#!/usr/bin/env python3
"""
快手移动端模拟环境 (CDP)。
在真实 Chrome 上注入 iPhone/安卓 设备指纹，并拦截 GraphQL/XHR 响应，
用于抓取移动端才能访问的快手作品列表。

Usage:
  python3 ks_mobile.py probe                 # 环境自检(UA/视口/登录态)
  python3 ks_mobile.py login                 # 打开登录页(供扫码)
  python3 ks_mobile.py profile [userId]      # 打开主页并 dump 拦截到的 API 数据
  python3 ks_mobile.py capture <url> [--scroll N] [--out file.json]
  python3 ks_mobile.py export [userId]       # 全量抓取作品 -> raw/ks_duokong.json
"""
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request

import websocket

# 本机 CDP 端口不走系统代理
os.environ.setdefault("no_proxy", "*")
os.environ.setdefault("NO_PROXY", "*")

HOST, PORT = "127.0.0.1", int(os.environ.get("KS_PORT", "9223"))
BASE = f"http://{HOST}:{PORT}"
RAW = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "raw")

IPHONE_UA = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_6 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Mobile/15E148 Safari/604.1"
)
ANDROID_UA = (
    "Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
)
IPHONE_METRICS = {
    "width": 390, "height": 844, "deviceScaleFactor": 3, "mobile": True,
    "screenOrientation": {"type": "portraitPrimary", "angle": 0},
}
ANDROID_METRICS = {
    "width": 412, "height": 915, "deviceScaleFactor": 2.625, "mobile": True,
    "screenOrientation": {"type": "portraitPrimary", "angle": 0},
}


def _http(path, method="GET"):
    req = urllib.request.Request(f"{BASE}{path}", method=method)
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read().decode())


def list_tabs():
    return [t for t in _http("/json/list") if t.get("type") == "page"]


def new_tab(url=""):
    ep = f"/json/new?{urllib.parse.quote(url, safe='')}" if url else "/json/new"
    return _http(ep, method="PUT")


class MobileTab:
    """带移动端指纹 + 网络拦截的 CDP 标签页。"""

    def __init__(self, ws_url, device="iphone"):
        self.ws = websocket.create_connection(ws_url, timeout=120, suppress_origin=True)
        self._id = 0
        self.net = []
        self._net_enabled = False
        self.device = device
        self._emit_counter = 0

    # ---- 基础 CDP ----
    def cmd(self, method, params=None, timeout=120, raw=False):
        self._id += 1
        self.ws.settimeout(timeout)
        self.ws.send(json.dumps({"id": self._id, "method": method, "params": params or {}}))
        while True:
            msg = json.loads(self.ws.recv())
            if msg.get("id") == self._id:
                if "error" in msg:
                    raise RuntimeError(msg["error"])
                return msg.get("result", {})
            self._on_event(msg)

    def _on_event(self, msg):
        m = msg.get("method")
        if not m:
            return
        if m == "Network.responseReceived" and self._net_enabled:
            p = msg["params"]
            url = p.get("response", {}).get("url", "")
            if re.search(r"/graphql|/rest/|/api/|profile|vision|feed|photo", url, re.I):
                self.net.append(
                    {
                        "requestId": p["requestId"],
                        "type": p.get("type"),
                        "url": url,
                        "status": p.get("response", {}).get("status"),
                        "mime": p.get("response", {}).get("mimeType"),
                    }
                )
        elif m == "Network.loadingFinished" and self._net_enabled:
            # 只打标记，禁止在此处发起新的 cmd（会造成 cmd 嵌套 -> 消息 ID 错乱死锁）
            for e in self.net:
                if e["requestId"] == msg["params"].get("requestId"):
                    e.setdefault("need_body", True)

    def drain(self, seconds=0.4):
        """消化挂起事件，再统一补齐响应体。"""
        end = time.time() + seconds
        self.ws.settimeout(0.5)
        while time.time() < end:
            try:
                msg = json.loads(self.ws.recv())
            except Exception:
                break
            self._on_event(msg)
        self.ws.settimeout(120)
        self.fetch_bodies()

    def fetch_bodies(self):
        """在 cmd 循环之外统一拉取响应体。"""
        for e in self.net:
            if e.get("need_body") and "body" not in e:
                try:
                    r = self.cmd("Network.getResponseBody", {"requestId": e["requestId"]}, timeout=20)
                    e["body"] = r.get("body", "")
                    e["base64"] = r.get("base64Encoded", False)
                except Exception as ex:
                    e["body"] = f"<err {type(ex).__name__}>"

    # ---- 移动端指纹 ----
    def emulate(self, device=None):
        device = device or self.device
        if device == "desktop":
            return None  # 保持浏览器原生桌面指纹
        ua = IPHONE_UA if device == "iphone" else ANDROID_UA
        metrics = IPHONE_METRICS if device == "iphone" else ANDROID_METRICS
        self.cmd("Emulation.setUserAgentOverride", {
            "userAgent": ua,
            "platform": "iPhone" if device == "iphone" else "Linux armv8l",
            "acceptLanguage": "zh-CN,zh;q=0.9",
        })
        self.cmd("Emulation.setDeviceMetricsOverride", metrics)
        self.cmd("Emulation.setTouchEmulationEnabled", {"enabled": True, "maxTouchPoints": 5})
        self.cmd("Emulation.setEmitTouchEventsForMouse", {"enabled": True, "configuration": "mobile"})
        return ua

    def enable_net(self):
        self.cmd("Network.enable", {"maxPostDataSize": 65536})
        self._net_enabled = True

    def navigate(self, url, wait=8, timeout=35):
        """导航并以 load 事件为准等待（避免固定 sleep 卡死）。"""
        self.cmd("Page.enable")
        self.cmd("Runtime.enable")
        self.cmd("Page.navigate", {"url": url})
        end = time.time() + timeout
        loaded = False
        self.ws.settimeout(1.0)
        while time.time() < end:
            try:
                msg = json.loads(self.ws.recv())
            except Exception:
                continue
            self._on_event(msg)
            if msg.get("method") == "Page.loadEventFired":
                loaded = True
                break
        self.ws.settimeout(120)
        time.sleep(min(wait, 6))
        self.drain(0.5)
        return loaded

    def eval(self, js, timeout=90):
        r = self.cmd("Runtime.evaluate",
                     {"expression": js, "returnByValue": True, "awaitPromise": True},
                     timeout=timeout)
        if r.get("exceptionDetails"):
            raise RuntimeError(r["exceptionDetails"])
        return r.get("result", {}).get("value")

    def text(self):
        return self.eval("document.body ? document.body.innerText : ''") or ""

    def scroll(self, times=8, pause=1.6):
        for _ in range(times):
            self.eval("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(pause)
            self.drain(0.3)

    def tap(self, x, y):
        self.cmd("Input.dispatchTouchEvent", {
            "type": "touchStart",
            "touchPoints": [{"x": x, "y": y}],
        })
        self.cmd("Input.dispatchTouchEvent", {"type": "touchEnd", "touchPoints": []})

    def screenshot(self, path):
        r = self.cmd("Page.captureScreenshot", {"format": "png"})
        import base64
        with open(path, "wb") as f:
            f.write(base64.b64decode(r["data"]))
        return path

    def close(self):
        try:
            self.ws.close()
        except Exception:
            pass


def open_mobile(url, device="iphone", wait=8, net=True):
    t = new_tab("about:blank")
    time.sleep(0.5)
    tab = MobileTab(t["webSocketDebuggerUrl"], device=device)
    tab.emulate(device)
    if net:
        tab.enable_net()
    tab.navigate(url, wait=wait)
    return t["id"], tab


def find_tab(keyword):
    for t in list_tabs():
        if keyword in (t.get("url") or ""):
            return t
    return None


def attach(keyword, device="iphone"):
    t = find_tab(keyword)
    if not t:
        return None, None
    tab = MobileTab(t["webSocketDebuggerUrl"], device=device)
    tab.emulate(device)
    return t["id"], tab


UA_CHECK = """(() => JSON.stringify({
  ua: navigator.userAgent,
  platform: navigator.platform,
  touch: navigator.maxTouchPoints,
  vw: innerWidth, vh: innerHeight, dpr: devicePixelRatio,
  url: location.href,
  cookieLen: document.cookie.length,
  hasDid: /did=|kuaishou|kpf=|userId/i.test(document.cookie),
  text: (document.body?document.body.innerText:'').slice(0,400)
}))()"""


def main():
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        return
    cmd = args[0]

    def opt(name, default=None):
        return args[args.index(name) + 1] if name in args else default

    if cmd == "probe":
        url = opt("--url", "https://m.kuaishou.com/")
        tid, tab = open_mobile(url, device=opt("--device", "iphone"), wait=float(opt("--wait", 10)))
        try:
            print(json.dumps(json.loads(tab.eval(UA_CHECK)), ensure_ascii=False, indent=2))
            print(f"\n--- 拦截到 {len(tab.net)} 个 API 请求 ---")
            for e in tab.net[:15]:
                print(f"  [{e.get('status')}] {e['url'][:110]}")
        finally:
            tab.close()
        return

    if cmd == "login":
        tid, tab = open_mobile("https://m.kuaishou.com/", wait=10)
        print("已打开移动端登录页，请在该 Chrome 窗口扫码登录。")
        print("URL:", "https://m.kuaishou.com/")
        print(json.dumps(json.loads(tab.eval(UA_CHECK)), ensure_ascii=False, indent=2))
        return  # 保持标签页打开

    if cmd == "capture":
        url = args[1]
        scroll_n = int(opt("--scroll", 6))
        out = opt("--out")
        tid, tab = open_mobile(url, wait=float(opt("--wait", 12)))
        try:
            tab.scroll(scroll_n)
            print(tab.text()[:2000])
            print(f"\n--- API 响应 {len(tab.net)} 条 ---")
            payload = []
            for e in tab.net:
                b = e.get("body", "")
                if b and len(b) > 50:
                    payload.append({"url": e["url"], "body": b[:200000]})
                print(f"  [{e.get('status')}] {len(b)}B {e['url'][:100]}")
            if out:
                p = os.path.join(RAW, out)
                with open(p, "w", encoding="utf-8") as f:
                    json.dump(payload, f, ensure_ascii=False, indent=1)
                print("已保存:", p)
        finally:
            tab.close()
        return

    print(__doc__)


if __name__ == "__main__":
    import urllib.parse  # noqa: E402
    main()
