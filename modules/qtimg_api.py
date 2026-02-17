import requests

def get_quote(code):
    url = f"http://qt.gtimg.cn/q={code}"
    r = requests.get(url, timeout=5)
    data = r.text.split("~")
    return {
        "name": data[1],
        "price": float(data[3])
    }
