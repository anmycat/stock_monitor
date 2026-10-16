#!/usr/bin/env python3
"""
快手移动端登录助手。

  python3 ks_login.py open     # 打开移动端登录页（保持窗口，供扫码）
  python3 ks_login.py check    # 检查当前登录态
  python3 ks_login.py cookie   # 导出 cookie -> raw/ks_cookie.json
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ks_mobile as K  # noqa: E402

RAW = K.RAW
LOGIN_URL = "https://www.kuaishou.com/?isHome=1"

CHECK_JS = """(() => {
  const c = document.cookie;
  const body = (document.body ? document.body.innerText : '').slice(0, 500);
  const avatar = !!document.querySelector('[class*=avatar],[class*=Avatar],img[class*=head]');
  return JSON.stringify({
    url: location.href,
    cookieLen: c.length,
    hasLoginCookie: /kpf=|userId=|kuaishou\\.server|sid=/i.test(c),
    cookieKeys: c.split(';').map(s=>s.trim().split('=')[0]).filter(Boolean).slice(0,25),
    hasLoginBtn: /登录|注册/.test(body),
    avatar,
    body: body.replace(/\\s+/g,' ').slice(0,300)
  });
})()"""


def find_ks_tab():
    for t in K.list_tabs():
        if "kuaishou" in (t.get("url") or ""):
            return t
    return None


def cmd_open(device="android"):
    t = find_ks_tab()
    if t:
        tid = t["id"]
        tab = K.MobileTab(t["webSocketDebuggerUrl"], device=device)
    else:
        _, tab = K.open_mobile(LOGIN_URL, device=device, wait=8)
        tid = None
    tab.emulate(device)
    tab.cmd("Page.bringToFront")
    print(json.dumps(json.loads(tab.eval(CHECK_JS)), ensure_ascii=False, indent=2))
    print("\n>>> 请在该 Chrome 窗口完成登录（扫码 / 手机号验证码）")
    print(">>> 登录后运行: python3 scripts/ks_login.py check")
    return tab


def cmd_check(device="android"):
    t = find_ks_tab()
    if not t:
        print("未找到快手标签页，先运行: python3 scripts/ks_login.py open")
        return None
    tab = K.MobileTab(t["webSocketDebuggerUrl"], device=device)
    try:
        tab.cmd("Page.bringToFront")
        info = json.loads(tab.eval(CHECK_JS))
        print(json.dumps(info, ensure_ascii=False, indent=2))
        logined = bool(info.get("hasLoginCookie")) and not info.get("hasLoginBtn")
        print("\n登录态:", "已登录 OK" if logined else "未登录 / 需扫码")
        return tab if logined else None
    finally:
        tab.close()


def cmd_cookie(device="android"):
    t = find_ks_tab()
    if not t:
        print("未找到快手标签页")
        return
    tab = K.MobileTab(t["webSocketDebuggerUrl"], device=device)
    try:
        cookies = tab.cmd("Network.getCookies", {"urls": ["https://www.kuaishou.com/"]})
        data = cookies.get("cookies", [])
        p = os.path.join(RAW, "ks_cookie.json")
        with open(p, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=1)
        print(f"已保存 {len(data)} 条 cookie -> {p}")
        for c in data:
            print(f"  {c['name']:<22} {str(c['value'])[:40]}")
    finally:
        tab.close()


if __name__ == "__main__":
    args = sys.argv[1:]
    cmd = args[0] if args else "check"
    dev = args[1] if len(args) > 1 else "android"
    if cmd == "open":
        cmd_open(dev)
    elif cmd == "check":
        cmd_check(dev)
    elif cmd == "cookie":
        cmd_cookie(dev)
    else:
        print(__doc__)
