"""
DSA (Daily Stock Analysis) 集成模块

提供与DSA系统的集成功能：
1. 获取DSA股票分析结果
2. 读取DSA生成的行情数据
3. 配置同步
"""

import os
import json
import logging
from typing import Dict, List, Optional
from pathlib import Path

logger = logging.getLogger("guardian")

DSA_BASE_PATH = os.getenv("DAILY_STOCK_ANALYSIS_REPO_PATH", "/daily_stock_analysis")
DSA_DATA_PATH = os.getenv(
    "DAILY_STOCK_ANALYSIS_QUOTE_PATH", "data/daily_stock_analysis/latest_quote.json"
)


def get_dsa_quote_data() -> Dict:
    """获取DSA生成的行情数据"""
    quote_path = DSA_DATA_PATH

    if not os.path.isabs(quote_path):
        quote_path = os.path.join(os.path.dirname(__file__), "..", quote_path)

    try:
        with open(quote_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        logger.info("dsa_quote_data_loaded stocks=%d", len(data))
        return data
    except Exception as e:
        logger.warning("dsa_quote_data_load_failed err=%s", e)
        return {}


def get_dsa_stock_info(stock_code: str) -> Optional[Dict]:
    """从DSA行情数据中获取股票信息"""
    quote_data = get_dsa_quote_data()
    return quote_data.get(stock_code)


def get_dsa_analysis_available() -> bool:
    """检查DSA服务是否可用"""
    server_path = os.path.join(DSA_BASE_PATH, "server.py")
    return os.path.exists(server_path)


def get_watchlist_from_dsa() -> List[Dict]:
    """从DSA配置获取自选股列表"""
    config_paths = [
        os.path.join(DSA_BASE_PATH, "config", "watchlist.json"),
        os.path.join(DSA_BASE_PATH, "config", "stocks.json"),
    ]

    for config_path in config_paths:
        try:
            if os.path.exists(config_path):
                with open(config_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                stocks = data.get("stocks", data.get("watchlist", []))
                if stocks:
                    logger.info(
                        "dsa_watchlist_loaded count=%d from %s",
                        len(stocks),
                        config_path,
                    )
                    return stocks
        except Exception as e:
            logger.debug("dsa_config_read_failed path=%s err=%s", config_path, e)

    return []


def merge_watchlist_configs(local_config: Dict, dsa_config: List[Dict]) -> Dict:
    """合并本地和DSA的配置

    优先级：本地配置 > DSA配置
    """
    local_stocks = local_config.get("watch_stocks", [])
    local_codes = {s.get("code") for s in local_stocks if s.get("code")}

    # 添加DSA中不在本地配置的股票
    for stock in dsa_config:
        code = stock.get("code") or stock.get("stock_code")
        if code and code not in local_codes:
            local_stocks.append(
                {
                    "code": code,
                    "name": stock.get("name", code),
                    "type": stock.get("type", "stock"),
                    "source": "dsa",
                }
            )
            local_codes.add(code)

    local_config["watch_stocks"] = local_stocks
    return local_config


def get_dsa_model_list() -> List[str]:
    """获取DSA支持的模型列表"""
    config_path = os.path.join(DSA_BASE_PATH, "config", "model_config.json")

    try:
        if os.path.exists(config_path):
            with open(config_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data.get("available_models", [])
    except Exception as e:
        logger.debug("dsa_model_config_read_failed err=%s", e)

    # 默认模型列表
    return ["gpt-4o-mini", "gpt-4o", "claude-3-sonnet", "gemini-1.5-pro"]


def check_dsa_health() -> Dict:
    """检查DSA系统健康状态"""
    health = {
        "available": get_dsa_analysis_available(),
        "quote_data": bool(get_dsa_quote_data()),
        "base_path": DSA_BASE_PATH,
    }

    if health["available"]:
        try:
            import sys

            sys.path.insert(0, DSA_BASE_PATH)
            health["server_importable"] = True
        except Exception as e:
            health["server_importable"] = False
            health["import_error"] = str(e)

    return health
