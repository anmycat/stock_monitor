from flask import Flask, jsonify
from modules.etf_monitor import get_etf_flows

app = Flask(__name__)

@app.route("/")
def home():
    return "Stock Monitor Web Dashboard Running"

@app.route("/etf")
def etf():
    return jsonify(get_etf_flows())

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
