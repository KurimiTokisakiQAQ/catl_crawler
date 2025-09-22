import requests
import json
import sys
from typing import List, Dict, Optional


class CityCrawler:
    def __init__(self, use_proxy=False, proxy_url="10.121.196.239:9090", verify_ssl=True, timeout=15):
        self.use_proxy = use_proxy
        self.proxy_url = proxy_url
        self.verify_ssl = verify_ssl
        self.timeout = timeout
        self.proxies = {"http": proxy_url, "https": proxy_url} if use_proxy else None

    def fetch_city_info(self) -> Optional[Dict]:
        """获取城市信息原始数据"""
        url = "https://c-gw-prod.chocolateswap.com/ps-base-api/area/manage/queryCityInfo"
        payload = {"operateStatus": 1}

        headers = {
            "ps-mode-type": "1",
            "User-Agent": "Mozilla/5.0 (Linux; Android 10; GM1910 Build/QKQ1.190716.003; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/85.0.4183.101 Mobile Safari/537.36kWebUserAgent.bsapp_android",
            "Content-Type": "application/json",
            "Accept": "*/*",
            "Origin": "https://static.chocolateswap.com",
            "Referer": "https://static.chocolateswap.com/pages/subPackageFeature/city-select/index",
            "X-Requested-With": "com.caes.choco.bs"
        }

        try:
            resp = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=self.timeout,
                proxies=self.proxies,
                verify=self.verify_ssl
            )
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            print(f"请求失败: {e}", file=sys.stderr)
            return None
        except ValueError:
            print("返回的不是 JSON", file=sys.stderr)
            return None

    def get_cities_list(self) -> List[Dict]:
        """获取并返回城市数据列表"""
        print("开始获取城市信息...")

        result = self.fetch_city_info()
        if result is None:
            print("获取城市信息失败")
            return []

        # 检查业务码
        if result.get("code") != 10000:
            print(f"业务返回码异常：{result.get('code')}，msg={result.get('msg')}")
            return []

        # 提取城市列表
        cities = []
        for group in result.get("data", []):
            for city in group.get("areaInfoDtoList", []):
                cities.append({
                    "cityName": city.get("cityName"),
                    "cityCode": city.get("cityCode"),
                    "cityLat": city.get("cityLat"),
                    "cityLng": city.get("cityLng"),
                    "provinceName": city.get("provinceName")
                })

        print(f"成功获取 {len(cities)} 个城市信息")
        return cities


# 提供向后兼容的函数
def get_cities_list(use_proxy=False, verify_ssl=True, timeout=15):
    """兼容旧版本的函数"""
    crawler = CityCrawler(use_proxy=use_proxy, verify_ssl=verify_ssl, timeout=timeout)
    return crawler.get_cities_list()


# 如果单独运行，只获取并打印城市信息
if __name__ == "__main__":
    crawler = CityCrawler(use_proxy=False, verify_ssl=True, timeout=15)
    cities = crawler.get_cities_list()
    if cities:
        print(f"成功获取 {len(cities)} 个城市")
        for i, city in enumerate(cities[:]):
            print(f"{i + 1}. {city['provinceName']}-{city['cityName']} (代码: {city['cityCode']})")
    else:
        print("获取城市信息失败")
        sys.exit(1)