import requests
import json
import time
import sys
import os
from typing import List, Dict, Optional

# 导入城市爬虫功能
try:
    from city_crawler import CityCrawler, get_cities_list
except ImportError:
    # 如果导入失败，定义备用函数
    def get_cities_list(use_proxy=False, verify_ssl=True, timeout=15):
        """获取城市数据的备用实现"""
        print("城市爬虫模块不可用，请确保 city_crawler.py 在同一目录下")
        return []

# 导入 Kafka 发送功能（可选）
try:
    from msg2kafka import send_station_list_message
except ImportError:
    send_station_list_message = None


class StationCrawler:
    def __init__(self, use_proxy=False, proxy_url="10.121.196.239:9090", verify_ssl=True, timeout=15):
        self.base_url = "https://c-gw-prod.chocolateswap.com/station/search/queryStationList"
        self.use_proxy = use_proxy
        self.proxy_url = proxy_url
        self.verify_ssl = verify_ssl
        self.timeout = timeout

        # 固定参数
        self.fixed_params = {
            "sortType": 1,
            "uid": "400183577804986524",
            "channelId": 6,
            "pageIndex": 1,
            "pageSize": 100
        }

        # 请求头
        self.headers = {
            "Host": "c-gw-prod.chocolateswap.com",
            "Connection": "keep-alive",
            "Content-Type": "application/json",
            "Accept": "*/*",
            "Origin": "https://static.chocolateswap.com",
            "X-Requested-With": "com.caes.choco.bs",
            "Sec-Fetch-Site": "same-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Referer": "https://static.chocolateswap.com/pages/home/index",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "User-Agent": "Mozilla/5.0 (Linux; Android 10; GM1910 Build/QKQ1.190716.003; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/85.0.4183.101 Mobile Safari/537.36kWebUserAgent.bsapp_android"
        }

        self.proxies = {"http": proxy_url, "https": proxy_url} if use_proxy else None

    def fetch_stations_for_city(self, city_info: Dict) -> Optional[Dict]:
        """获取单个城市的站点列表"""
        payload = {
            **self.fixed_params,
            "lat": city_info["cityLat"],
            "lng": city_info["cityLng"],
            "cityCode": city_info["cityCode"]
        }

        try:
            response = requests.post(
                self.base_url,
                headers=self.headers,
                json=payload,
                timeout=self.timeout,
                proxies=self.proxies,
                verify=self.verify_ssl
            )
            response.raise_for_status()

            result = response.json()
            if result.get("code") == 10000:
                return result
            else:
                print(f"城市 {city_info['cityName']} 请求失败: code={result.get('code')}, msg={result.get('msg')}")
                return None

        except requests.RequestException as e:
            print(f"城市 {city_info['cityName']} 请求异常: {e}")
            return None
        except ValueError:
            print(f"城市 {city_info['cityName']} 返回数据解析失败")
            return None

    def crawl_all_cities(self, cities_data: List[Dict], delay: float = 1.0) -> Dict[str, Dict]:
        """遍历所有城市获取站点信息"""
        all_stations = {}
        total_cities = len(cities_data)

        print(f"开始爬取 {total_cities} 个城市的站点信息...")

        for i, city in enumerate(cities_data, 1):
            city_name = city["cityName"]
            print(f"[{i}/{total_cities}] 正在获取 {city_name} 的站点信息...")

            result = self.fetch_stations_for_city(city)
            if result:
                all_stations[city["cityCode"]] = {
                    "city_info": {
                        "cityName": city["cityName"],
                        "cityCode": city["cityCode"],
                        "provinceName": city["provinceName"]
                    },
                    "station_data": result
                }
                print(f"  √ 成功获取 {city_name} 的站点信息")

                # 发送 Kafka（每个城市一条消息）
                if send_station_list_message is not None:
                    try:
                        payload = {
                            "city_info": all_stations[city["cityCode"]]["city_info"],
                            "station_data": result
                        }
                        ok = send_station_list_message(payload)
                        if not ok:
                            print(f"  × 发送 {city_name} 站点列表到 Kafka 失败")
                    except Exception as _:
                        print(f"  × 发送 {city_name} 站点列表到 Kafka 异常")
            else:
                print(f"  × 获取 {city_name} 站点信息失败")

            # 添加延迟
            if i < total_cities:
                time.sleep(delay)

        return all_stations

    def get_all_stations(self) -> Dict[str, Dict]:
        """获取所有城市的站点信息（包含城市数据获取）"""
        # 获取城市列表
        print("正在获取城市列表...")
        cities = get_cities_list(use_proxy=self.use_proxy, verify_ssl=self.verify_ssl, timeout=self.timeout)

        if not cities:
            print("获取城市信息失败，程序退出")
            return {}

        print(f"成功获取 {len(cities)} 个城市信息")

        # 开始爬取所有城市的站点信息
        return self.crawl_all_cities(cities, delay=0.5)


def get_stations_data(use_proxy=False, verify_ssl=True, timeout=15) -> Dict[str, Dict]:
    """直接获取站点数据的函数"""
    crawler = StationCrawler(use_proxy=use_proxy, verify_ssl=verify_ssl, timeout=timeout)
    return crawler.get_all_stations()


if __name__ == "__main__":
    # 创建爬虫实例
    crawler = StationCrawler(
        use_proxy=False,
        proxy_url="10.121.196.239:9090",
        verify_ssl=True,
        timeout=15
    )

    # 获取所有站点数据
    all_stations = crawler.get_all_stations()

    # 统计信息
    if all_stations:
        successful_cities = len(all_stations)
        total_stations = 0
        for city_data in all_stations.values():
            station_list = city_data["station_data"].get("data", {}).get("stationList", [])
            total_stations += len(station_list)

        print(f"\n爬取完成！成功获取 {successful_cities} 个城市的 {total_stations} 个站点信息")
    else:
        print("没有成功获取任何城市的站点信息")