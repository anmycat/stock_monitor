"""
相关性风险控制管理器 - 避免持仓过度集中在相关板块

功能:
- 持仓板块分布监控
- 相关性矩阵计算
- 板块集中度限制(单板块≤30%)
- 风险分散度评估
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from loguru import logger


@dataclass
class SectorExposure:
    """板块风险暴露"""
    sector_name: str
    position_value: float
    position_pct: float  # 占总仓位比例
    stock_count: int
    risk_level: str  # low/medium/high


@dataclass
class CorrelationReport:
    """相关性分析报告"""
    avg_correlation: float  # 平均相关性
    max_correlation: float  # 最高相关性
    min_correlation: float  # 最低相关性
    high_corr_pairs: List[Tuple[str, str, float]]  # 高相关性股票对
    diversification_score: float  # 分散度评分(0-100)
    risk_warning: str  # 风险警告


class CorrelationManager:
    """相关性风险控制管理器"""
    
    def __init__(self):
        # 板块集中度限制
        self.max_single_sector_pct = 0.30  # 单板块最大30%
        self.max_top3_sector_pct = 0.60    # 前三大板块最大60%
        
        # 相关性阈值
        self.high_correlation_threshold = 0.7  # 高相关性阈值
        self.medium_correlation_threshold = 0.5  # 中等相关性阈值
        
        # 板块分类
        self.sector_mapping = self._init_sector_mapping()
    
    def _init_sector_mapping(self) -> Dict[str, List[str]]:
        """初始化板块映射"""
        return {
            '科技': ['688', '002', '300'],  # 科创板、中小板、创业板
            '金融': ['601', '000'],  # 银行、券商
            '消费': ['600', '000'],  # 食品饮料、家电
            '医药': ['300', '600'],  # 医药生物
            '能源': ['601', '002'],  # 石油、煤炭
            '工业': ['600', '601'],  # 机械制造
            '地产': ['600', '000'],  # 房地产
            '材料': ['600', '002'],  # 钢铁、化工
        }
    
    def analyze_sector_exposure(self, positions: List[Dict]) -> List[SectorExposure]:
        """
        分析板块风险暴露
        
        Args:
            positions: 持仓列表 [{code, name, value, sector}, ...]
            
        Returns:
            板块风险暴露列表
        """
        if not positions:
            return []
        
        # 按板块统计
        sector_stats = {}
        total_value = sum(p.get('value', 0) for p in positions)
        
        for position in positions:
            sector = position.get('sector', '未知')
            value = position.get('value', 0)
            
            if sector not in sector_stats:
                sector_stats[sector] = {
                    'value': 0,
                    'count': 0
                }
            
            sector_stats[sector]['value'] += value
            sector_stats[sector]['count'] += 1
        
        # 生成报告
        exposures = []
        for sector, stats in sector_stats.items():
            pct = stats['value'] / total_value if total_value > 0 else 0
            
            # 风险等级
            if pct > self.max_single_sector_pct:
                risk_level = 'high'
            elif pct > 0.20:
                risk_level = 'medium'
            else:
                risk_level = 'low'
            
            exposures.append(SectorExposure(
                sector_name=sector,
                position_value=stats['value'],
                position_pct=round(pct, 4),
                stock_count=stats['count'],
                risk_level=risk_level
            ))
        
        # 按仓位降序
        exposures.sort(key=lambda x: x.position_pct, reverse=True)
        
        return exposures
    
    def check_sector_concentration(self, positions: List[Dict]) -> Dict:
        """
        检查板块集中度风险
        
        Returns:
            集中度检查结果
        """
        exposures = self.analyze_sector_exposure(positions)
        
        if not exposures:
            return {
                'is_concentrated': False,
                'risk_level': 'low',
                'warnings': []
            }
        
        warnings = []
        
        # 检查单板块集中度
        for exposure in exposures:
            if exposure.position_pct > self.max_single_sector_pct:
                warnings.append(
                    f"板块 '{exposure.sector_name}' 仓位 {exposure.position_pct:.2%} "
                    f"超过限制 {self.max_single_sector_pct:.0%}"
                )
        
        # 检查前三大板块集中度
        top3_pct = sum(e.position_pct for e in exposures[:3])
        if top3_pct > self.max_top3_sector_pct:
            warnings.append(
                f"前三大板块集中度 {top3_pct:.2%} 超过限制 {self.max_top3_sector_pct:.0%}"
            )
        
        risk_level = 'high' if len(warnings) >= 2 else 'medium' if warnings else 'low'
        
        return {
            'is_concentrated': len(warnings) > 0,
            'risk_level': risk_level,
            'warnings': warnings,
            'top_sectors': [(e.sector_name, e.position_pct) for e in exposures[:3]]
        }
    
    def calculate_correlation_matrix(self, price_data: pd.DataFrame) -> pd.DataFrame:
        """
        计算股票相关性矩阵
        
        Args:
            price_data: 价格数据 DataFrame(columns=股票codes, index=日期)
            
        Returns:
            相关性矩阵
        """
        if price_data.empty or len(price_data) < 30:
            logger.warning("价格数据不足,无法计算相关性")
            return pd.DataFrame()
        
        # 计算收益率
        returns = price_data.pct_change().dropna()
        
        # 计算相关性矩阵
        corr_matrix = returns.corr()
        
        return corr_matrix
    
    def analyze_correlation_risk(
        self, 
        positions: List[Dict], 
        price_data: Optional[pd.DataFrame] = None
    ) -> CorrelationReport:
        """
        分析相关性风险
        
        Args:
            positions: 持仓列表
            price_data: 价格数据(可选)
            
        Returns:
            相关性分析报告
        """
        codes = [p['code'] for p in positions]
        
        if not codes or price_data is None or price_data.empty:
            return CorrelationReport(
                avg_correlation=0.0,
                max_correlation=0.0,
                min_correlation=0.0,
                high_corr_pairs=[],
                diversification_score=50,
                risk_warning="数据不足,无法分析相关性"
            )
        
        # 计算相关性矩阵
        corr_matrix = self.calculate_correlation_matrix(price_data[codes])
        
        if corr_matrix.empty:
            return CorrelationReport(
                avg_correlation=0.0,
                max_correlation=0.0,
                min_correlation=0.0,
                high_corr_pairs=[],
                diversification_score=50,
                risk_warning="相关性计算失败"
            )
        
        # 提取上三角矩阵(去除对角线)
        n = len(corr_matrix)
        correlations = []
        high_corr_pairs = []
        
        for i in range(n):
            for j in range(i + 1, n):
                corr = corr_matrix.iloc[i, j]
                correlations.append(corr)
                
                code1 = corr_matrix.index[i]
                code2 = corr_matrix.columns[j]
                
                if corr > self.high_correlation_threshold:
                    high_corr_pairs.append((code1, code2, round(corr, 3)))
        
        if not correlations:
            return CorrelationReport(
                avg_correlation=0.0,
                max_correlation=0.0,
                min_correlation=0.0,
                high_corr_pairs=[],
                diversification_score=50,
                risk_warning="持仓股票不足"
            )
        
        # 统计指标
        avg_corr = np.mean(correlations)
        max_corr = np.max(correlations)
        min_corr = np.min(correlations)
        
        # 计算分散度评分
        diversification_score = self._calculate_diversification_score(
            avg_corr, len(codes), len(high_corr_pairs)
        )
        
        # 风险警告
        risk_warning = self._generate_correlation_warning(
            avg_corr, max_corr, high_corr_pairs
        )
        
        return CorrelationReport(
            avg_correlation=round(avg_corr, 3),
            max_correlation=round(max_corr, 3),
            min_correlation=round(min_corr, 3),
            high_corr_pairs=high_corr_pairs[:10],  # 最多返回10对
            diversification_score=diversification_score,
            risk_warning=risk_warning
        )
    
    def _calculate_diversification_score(
        self, 
        avg_corr: float, 
        stock_count: int,
        high_corr_count: int
    ) -> float:
        """
        计算分散度评分(0-100)
        
        评分越高,分散越好
        """
        score = 50  # 基础分
        
        # 平均相关性影响
        if avg_corr < 0.3:
            score += 20
        elif avg_corr < 0.5:
            score += 10
        elif avg_corr > 0.7:
            score -= 20
        elif avg_corr > 0.5:
            score -= 10
        
        # 股票数量影响
        if stock_count >= 10:
            score += 15
        elif stock_count >= 5:
            score += 10
        elif stock_count < 3:
            score -= 15
        
        # 高相关性股票对影响
        if high_corr_count == 0:
            score += 15
        elif high_corr_count <= 2:
            score += 5
        elif high_corr_count > 5:
            score -= 20
        
        return max(0, min(100, score))
    
    def _generate_correlation_warning(
        self,
        avg_corr: float,
        max_corr: float,
        high_corr_pairs: List
    ) -> str:
        """生成相关性风险警告"""
        warnings = []
        
        if avg_corr > 0.6:
            warnings.append(f"平均相关性过高({avg_corr:.2f})")
        elif avg_corr > 0.4:
            warnings.append(f"平均相关性偏高({avg_corr:.2f})")
        
        if max_corr > 0.8:
            warnings.append(f"存在极高相关性股票对({max_corr:.2f})")
        
        if high_corr_pairs:
            warnings.append(f"发现{len(high_corr_pairs)}对高相关性股票")
        
        return " | ".join(warnings) if warnings else "相关性风险可控"
    
    def get_rebalancing_suggestions(
        self, 
        positions: List[Dict],
        correlation_report: CorrelationReport
    ) -> List[str]:
        """
        获取调仓建议
        
        Returns:
            调仓建议列表
        """
        suggestions = []
        
        # 检查板块集中度
        concentration_check = self.check_sector_concentration(positions)
        
        if concentration_check['is_concentrated']:
            for warning in concentration_check['warnings']:
                suggestions.append(f"板块集中风险: {warning}")
            
            suggestions.append("建议: 减少高集中度板块持仓,增加其他板块配置")
        
        # 检查相关性
        if correlation_report.avg_correlation > 0.6:
            suggestions.append("建议: 持仓相关性过高,建议增加低相关性资产")
        
        if correlation_report.high_corr_pairs:
            pairs_str = ", ".join([f"{p[0]}-{p[1]}" for p in correlation_report.high_corr_pairs[:3]])
            suggestions.append(f"建议: 考虑减持高相关性股票对({pairs_str})")
        
        # 分散度建议
        if correlation_report.diversification_score < 40:
            suggestions.append("警告: 持仓分散度较差,建议增加持仓数量")
        
        return suggestions


# 全局相关性管理器
_correlation_manager = None

def get_correlation_manager() -> CorrelationManager:
    """获取全局相关性管理器"""
    global _correlation_manager
    if _correlation_manager is None:
        _correlation_manager = CorrelationManager()
    return _correlation_manager
