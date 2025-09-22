import requests
import json
import time
import sys
import os
from typing import Dict, List, Optional

# 导入站点爬虫功能
try:
    from station_crawler import get_stations_data
except ImportError:
    # 如果导入失败，定义备用函数
    def get_stations_data(use_proxy=False, verify_ssl=True, timeout=15):
        """获取站点数据的备用实现"""
        print("站点爬虫模块不可用，请确保 station_crawler.py 在同一目录下")
        return {}

# 导入 Kafka 发送功能（可选）
try:
    from msg2kafka import (
        init_kafka_producer,
        close_kafka_producer,
        send_station_detail_message,
    )
except ImportError:
    init_kafka_producer = None
    close_kafka_producer = None
    send_station_detail_message = None


class StationDetailCrawler:
    def __init__(self, use_proxy=False, proxy_url="10.121.196.239:9090", verify_ssl=True, timeout=15):
        self.base_url = "https://c-gw-prod.chocolateswap.com/station/search/queryStationDetail"
        self.use_proxy = use_proxy
        self.proxy_url = proxy_url
        self.verify_ssl = verify_ssl
        self.timeout = timeout

        # 固定参数
        self.fixed_params = {
            "uid": "400183577804986524",
            "channelId": 6
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
            "Referer": "https://static.chocolateswap.com/pages/station-details/index",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "User-Agent": "Mozilla/5.0 (Linux; Android 10; GM1910 Build/QKQ1.190716.003; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/85.0.4183.101 Mobile Safari/537.36kWebUserAgent.bsapp_android"
        }

        self.proxies = {"http": proxy_url, "https": proxy_url} if use_proxy else None

    def fetch_station_detail(self, station_info: Dict) -> Optional[Dict]:
        """获取单个站点的明细信息"""
        payload = {
            **self.fixed_params,
            "lat": station_info["stationLat"],
            "lng": station_info["stationLng"],
            "cityCode": station_info["cityCode"],
            "stationId": station_info["stationId"]
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
                print(f"站点 {station_info['stationId']} 请求失败: code={result.get('code')}, msg={result.get('msg')}")
                return None

        except requests.RequestException as e:
            print(f"站点 {station_info['stationId']} 请求异常: {e}")
            return None
        except ValueError:
            print(f"站点 {station_info['stationId']} 返回数据解析失败")
            return None

    def crawl_all_stations(self, stations_data: Dict[str, Dict], delay: float = 0.5) -> Dict[str, Dict]:
        """遍历所有站点获取明细信息"""
        all_station_details = {}
        total_stations = 0

        # 计算总站点数
        for city_code, city_data in stations_data.items():
            station_data = city_data.get("station_data", {})
            data_section = station_data.get("data", {})

            # 根据实际JSON结构调整字段名
            if "pageObject" in data_section:
                station_list = data_section["pageObject"]
            elif "stationList" in data_section:
                station_list = data_section["stationList"]
            elif "list" in data_section:
                station_list = data_section["list"]
            else:
                # 如果没有找到预期的字段，尝试获取第一个数组类型的值
                station_list = []
                for key, value in data_section.items():
                    if isinstance(value, list):
                        station_list = value
                        break

            total_stations += len(station_list)

        print(f"开始爬取 {total_stations} 个站点的明细信息...")

        current_station = 0
        for city_code, city_data in stations_data.items():
            station_data = city_data.get("station_data", {})
            data_section = station_data.get("data", {})

            # 同样的灵活处理方式
            if "pageObject" in data_section:
                station_list = data_section["pageObject"]
            elif "stationList" in data_section:
                station_list = data_section["stationList"]
            elif "list" in data_section:
                station_list = data_section["list"]
            else:
                station_list = []
                for key, value in data_section.items():
                    if isinstance(value, list):
                        station_list = value
                        break

            city_name = city_data["city_info"]["cityName"]

            print(f"\n处理城市: {city_name} ({len(station_list)} 个站点)")

            for station in station_list:
                current_station += 1
                station_id = station.get("stationId")
                station_name = station.get("stationName", "未知站点")

                print(f"[{current_station}/{total_stations}] 正在获取 {station_name} ({station_id}) 的明细信息...")

                # 准备站点信息
                station_info = {
                    "stationId": station_id,
                    "stationLat": station.get("stationLat"),
                    "stationLng": station.get("stationLng"),
                    "cityCode": city_code,
                    "stationName": station_name
                }

                result = self.fetch_station_detail(station_info)
                if result:
                    all_station_details[station_id] = {
                        "station_info": station_info,
                        "detail_data": result
                    }
                    print(f"  √ 成功获取 {station_name} 的明细信息")

                    # 发送 Kafka（每个站点一条消息）
                    if send_station_detail_message is not None:
                        try:
                            payload = {
                                "station_info": station_info,
                                "detail_data": result
                            }
                            ok = send_station_detail_message(payload)
                            if not ok:
                                print(f"  × 发送站点 {station_name} 明细到 Kafka 失败")
                        except Exception as _:
                            print(f"  × 发送站点 {station_name} 明细到 Kafka 异常")
                else:
                    print(f"  × 获取 {station_name} 明细信息失败")

                # 添加延迟
                if current_station < total_stations:
                    time.sleep(delay)

        return all_station_details

    def get_all_station_details(self) -> Dict[str, Dict]:
        """获取所有站点的明细信息（包含站点数据获取）"""
        # 获取站点列表数据
        print("正在获取站点列表数据...")
        stations_data = get_stations_data(
            use_proxy=self.use_proxy,
            verify_ssl=self.verify_ssl,
            timeout=self.timeout
        )

        if not stations_data:
            print("获取站点数据失败，程序退出")
            return {}

        print(f"成功获取 {len(stations_data)} 个城市的站点数据")

        # 开始爬取所有站点的明细信息
        return self.crawl_all_stations(stations_data, delay=0.3)


def main():
    """主函数：获取所有站点的明细信息"""
    # 创建爬虫实例
    crawler = StationDetailCrawler(
        use_proxy=False,
        proxy_url="10.121.196.239:9090",
        verify_ssl=True,
        timeout=15
    )

    # 初始化 Kafka 生产者（如果可用）
    if init_kafka_producer is not None:
        init_kafka_producer()

    # 获取所有站点详情数据
    all_station_details = crawler.get_all_station_details()

    # 统计信息
    if all_station_details:
        successful_stations = len(all_station_details)
        print(f"\n爬取完成！成功获取 {successful_stations} 个站点的明细信息")

        # 可以在这里添加数据保存或进一步处理的逻辑
        return all_station_details
    else:
        print("没有成功获取任何站点的明细信息")
        return {}


if __name__ == "__main__":
    result = main()
    # 关闭 Kafka 生产者（如果可用）
    if close_kafka_producer is not None:
        try:
            close_kafka_producer()
        except Exception:
            pass