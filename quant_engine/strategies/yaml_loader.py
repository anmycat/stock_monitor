"""YAML策略加载器 - 从YAML文件加载策略配置"""
from __future__ import annotations

from pathlib import Path

import yaml
from loguru import logger


class YamlStrategyLoader:
    """YAML策略加载器"""

    def __init__(self, strategies_dir: str | None = None):
        if strategies_dir is None:
            strategies_dir = str(Path(__file__).parent / "yaml")
        self.strategies_dir = Path(strategies_dir)
        self.strategies: dict[str, dict] = {}
        self._load_all()

    def _load_all(self):
        """加载所有YAML策略"""
        if not self.strategies_dir.exists():
            logger.warning(f"Strategies directory not found: {self.strategies_dir}")
            return

        for yaml_file in self.strategies_dir.glob("*.yaml"):
            try:
                strategy = self._load_strategy(yaml_file)
                if strategy and 'meta' in strategy:
                    strategy_id = strategy['meta'].get('id')
                    if strategy_id:
                        self.strategies[strategy_id] = strategy
                        logger.info(f"Loaded strategy: {strategy_id}")
            except Exception as e:  # noqa: BLE001 外部调用
                logger.error(f"Failed to load {yaml_file}: {e}")

        logger.info(f"Loaded {len(self.strategies)} YAML strategies")

    def _load_strategy(self, yaml_file: Path) -> dict | None:
        """加载单个YAML策略文件"""
        try:
            with open(yaml_file, 'r', encoding='utf-8') as f:
                strategy = yaml.safe_load(f)
            return strategy
        except (OSError, yaml.YAMLError) as e:
            logger.error(f"Parse YAML error: {e}")
            return None

    def get_strategy(self, strategy_id: str) -> dict | None:
        """获取策略配置"""
        return self.strategies.get(strategy_id)

    def list_strategies(self) -> list[dict]:
        """列出所有策略"""
        result = []
        for strategy_id, strategy in self.strategies.items():
            meta = strategy.get('meta', {})
            result.append({
                'id': strategy_id,
                'name': meta.get('name', ''),
                'category': meta.get('category', ''),
                'description': meta.get('description', ''),
            })
        return result

    def get_strategies_by_category(self, category: str) -> list[dict]:
        """按分类获取策略"""
        result = []
        for strategy_id, strategy in self.strategies.items():
            meta = strategy.get('meta', {})
            if meta.get('category') == category:
                result.append({
                    'id': strategy_id,
                    'name': meta.get('name', ''),
                    'description': meta.get('description', ''),
                })
        return result

    def reload(self):
        """重新加载所有策略"""
        self.strategies.clear()
        self._load_all()


# 全局实例
_loader = None


def get_yaml_loader() -> YamlStrategyLoader:
    """获取全局YAML加载器"""
    global _loader
    if _loader is None:
        _loader = YamlStrategyLoader()
    return _loader
