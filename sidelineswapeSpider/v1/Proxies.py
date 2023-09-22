import requests
import json
from pyquery import PyQuery

class Proxies:
    def __init__(self, pages, url='http://www.ip3366.net/?stype=1'):
        self.pages = pages
        self.url = url

    # 检验代理ip是否可用
    def verify_proxy(self, scheme, ip, port):
        proxy = {
            'http': scheme + '://' + ip + ':' + port + '/',
            'https': scheme + '://' + ip + ':' + port + '/'
        }
        try:
            response = requests.get('http://www.baidu.com', proxies=proxy, timeout=5)
            if response.status_code == 200:
                return proxy
        except:
            return None
    #爬取代理ip
    def crawl_proxy(self):
        pages = self.pages
        url = self.url
        proxies = []
        with open('useful_proxy.json', 'w+', encoding='utf-8') as f:
            for page in range(pages):
                page_url = url + f'&page={page}'
                print('crawl:', page_url)
                req = requests.get(url)
                req.encoding = 'gb2312'
                if req:
                    d = PyQuery(req.text)
                    trs = d('.table-bordered tbody tr').items()
                    for tr in trs:
                        scheme = tr.find('td:nth-child(4)').text().lower()
                        ip = tr.find('td:nth-child(1)').text()
                        port = tr.find('td:nth-child(2)').text()
                        proxy = self.verify_proxy(scheme, ip, port)
                        print(proxy)
                        if proxy:
                            proxies.append(proxy)
            f.write(json.dumps(proxies, ensure_ascii=False))
            return proxies

