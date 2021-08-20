# import pymysql
import requests
# from bs4 import BeautifulSoup
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import time
import os
import json
import datetime
import xmltodict

s = requests.Session()
retries = Retry(total=5,
               backoff_factor=4, # 2, 4, 8, 16, 32
               status_forcelist=[500, 502, 503, 504])
headers= {
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:87.0) Gecko/20100101 Firefox/87.0'
}

def crawl(base_url, params, directory, name):
    today = datetime.datetime.now()
    res = s.get(base_url, headers=headers, params=params)
    if res.status_code == 200:
        result = json.loads(json.dumps(xmltodict.parse(res.content.decode('utf-8')), ensure_ascii=False))
        with open(directory + today.strftime("%Y%m%d%H%M%S") + name + ".json", "w", encoding='utf-8') as json_file:
            json.dump(result, json_file, ensure_ascii=False)
        return result
    else:
        return res.status_code
keys = [
    'YjL6G/s7oQzWHKrVEGwnzln1KCpjgiVWaebEKg45eP2zjMcv5Mr3yhy7Yvw15A895SR6r3w9jdxViYi53J8q/w==',
    '91V/qqMLmutjLeSOgIGBCbM3qjKMDQT/pyJl7bNPHTSEtL3yDw95ISO09JulNndTy/O7fyBDf283Nh9RzoTlzw==',
    'P/A790R+948U9aRNFwyvH4s4J5rakjk7BJbSNZxKlDoqr9XI1kgbPO+CoH9EwJsJG8Ex4TaortyjJGid58aBAw==',
]
# cron1 bus
# 1분 간격
buses = [
    '100100019',
    '100100073',
    '123000010',
]
for i in range(3):
    base_url = 'http://ws.bus.go.kr/api/rest/buspos/getBusPosByRtid'
    params = {
        'busRouteId': buses[i],
        'serviceKey': keys[i]
    }
    crawl(base_url, params, './bus/', 'Bus' + buses[i])

