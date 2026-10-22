from typing import List, Dict
from loguru import logger
from ...data.repositories import MarketDataRepository


class SectorMonitor:
    def __init__(self, datacenter: MarketDataRepository):
        self.datacenter = datacenter
        self.sectors = {
            '银行': ['600036.SH','601166.SH','600000.SH','601398.SH','601288.SH','601939.SH','601328.SH','601818.SH','600015.SH','601988.SH','601998.SH','601009.SH','601229.SH','600919.SH','601838.SH','002142.SZ'],
            '白酒': ['600519.SH','000858.SZ','000568.SZ','002304.SZ','600809.SH','000799.SZ','603369.SH','600559.SH','000596.SZ','600779.SH','603589.SH','000860.SZ','600702.SH','600199.SH','002646.SZ','600197.SH'],
            '新能源': ['300750.SZ','002594.SZ','601012.SH','600438.SH','688303.SH','688599.SH','002460.SZ','300274.SZ','688390.SH','002812.SZ','300763.SZ','688349.SH','600406.SH','300438.SZ','688005.SH','002709.SZ'],
            '半导体': ['688981.SH','688036.SH','688012.SH','603501.SH','603986.SH','002049.SZ','688009.SH','688008.SH','002371.SZ','688041.SH','300661.SZ','688200.SH','603160.SH','688126.SH','688521.SH','002185.SZ'],
            '医药': ['600276.SH','300760.SZ','603259.SH','000538.SZ','600085.SH','002007.SZ','688180.SH','300122.SZ','000963.SZ','300347.SZ','002001.SZ','600196.SH','002252.SZ','300015.SZ','603882.SH','688185.SH'],
            '军工': ['600760.SH','600893.SH','601989.SH','000768.SZ','600150.SH','600862.SH','002179.SZ','600316.SH','000738.SZ','002025.SZ','600372.SH','601698.SH','002414.SZ','688066.SH','002013.SZ','600879.SH'],
            '证券': ['601211.SH','601688.SH','600030.SH','601066.SH','601881.SH','601788.SH','601236.SH','601377.SH','600958.SH','002736.SZ','601901.SH','601456.SH','601198.SH','000776.SZ','600837.SH','601878.SH'],
            '房地产': ['000002.SZ','600048.SH','001979.SZ','600383.SH','001914.SZ','600340.SH','000656.SZ','601155.SH','600376.SH','600266.SH','002244.SZ','600325.SH','000671.SZ','600208.SH','600606.SH','000031.SZ'],
            '家电': ['000333.SZ','000651.SZ','600690.SH','002032.SZ','603515.SH','000921.SZ','002508.SZ','002050.SZ','600060.SH','000404.SZ','002668.SZ','605117.SH','002705.SZ','002677.SZ','688169.SH','002242.SZ'],
            '汽车': ['600104.SH','000625.SZ','601238.SH','002594.SZ','601127.SH','600741.SH','601633.SH','002920.SZ','000800.SZ','600418.SH','002048.SZ','600699.SH','601799.SH','002126.SZ','600733.SH','601689.SH'],
            '计算机': ['688111.SH','603019.SH','002230.SZ','000977.SZ','600570.SH','688561.SH','688256.SH','002410.SZ','300496.SZ','002415.SZ','688188.SH','300253.SZ','002236.SZ','002065.SZ','688787.SH','300033.SZ'],
            '电子': ['002475.SZ','002241.SZ','600703.SH','688036.SH','002236.SZ','000725.SZ','002916.SZ','603986.SH','688012.SH','002938.SZ','688008.SH','002463.SZ','600584.SH','002138.SZ','688187.SH','300223.SZ'],
            '电力': ['600900.SH','601985.SH','600795.SH','600886.SH','688187.SH','600025.SH','601991.SH','600011.SH','600023.SH','600027.SH','600674.SH','600578.SH','003816.SZ','601611.SH','601669.SH','600642.SH'],
            '煤炭': ['601088.SH','601225.SH','600188.SH','601898.SH','600397.SH','600508.SH','000983.SZ','601666.SH','600740.SH','601001.SH','600348.SH','601699.SH','002128.SZ','600925.SH','600971.SH','601015.SH'],
            '有色': ['601899.SH','600111.SH','600489.SH','600362.SH','601600.SH','002460.SZ','600547.SH','601168.SH','000878.SZ','600259.SH','002466.SZ','600711.SH','600549.SH','601137.SH','603399.SH','000630.SZ'],
            '食品': ['600887.SH','603288.SH','600600.SH','000895.SZ','600132.SH','002568.SZ','600597.SH','603043.SH','002557.SZ','002507.SZ','002714.SZ','600298.SH','603517.SH','605499.SH','600791.SH','001215.SZ'],
        }

    def get_sector_data(self) -> List[Dict]:
        results = []
        for sector_name, codes in self.sectors.items():
            try:
                quotes = self.datacenter.get_realtime_quote(codes)
                if quotes.empty:
                    continue

                up_count = len(quotes[quotes['change_pct'] > 0])
                down_count = len(quotes[quotes['change_pct'] < 0])
                avg_change = quotes['change_pct'].mean()
                total = len(quotes)

                results.append({
                    'name': sector_name,
                    'avg_change': round(avg_change, 2),
                    'up_count': up_count,
                    'down_count': down_count,
                    'total': total,
                    'up_ratio': round(up_count / total * 100, 0) if total > 0 else 0,
                    'stocks': [{'code': row['code'], 'name': row.get('name', row['code']), 'change_pct': round(row['change_pct'], 2)} for _, row in quotes.sort_values('change_pct', ascending=False).iterrows()],
                })
            except Exception as e:  # noqa: BLE001 外部调用
                logger.error(f"Get sector {sector_name} error: {e}")

        results.sort(key=lambda x: abs(x['avg_change']), reverse=True)
        return results

    def get_hot_sectors(self, top_n: int = 5) -> List[Dict]:
        all_sectors = self.get_sector_data()
        up_sectors = [s for s in all_sectors if s['avg_change'] > 0]
        up_sectors.sort(key=lambda x: x['avg_change'], reverse=True)
        return up_sectors[:top_n]

    def get_cold_sectors(self, top_n: int = 5) -> List[Dict]:
        all_sectors = self.get_sector_data()
        down_sectors = [s for s in all_sectors if s['avg_change'] < 0]
        down_sectors.sort(key=lambda x: x['avg_change'])
        return down_sectors[:top_n]
