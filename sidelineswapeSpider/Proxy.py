import requests
from pyquery import PyQuery

class Proxy:
    def __init__(self, pages, headers, url='http://www.ip3366.net/?stype=1'):
        self.pages = pages
        self.headers = headers
        self.url = url

    #爬取代理ip
    def crawl_proxy(self, headers, verify_url, timeout=60):
        pages = self.pages
        url = self.url
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
                    body = self.verify_proxy(scheme, ip, port, headers, verify_url, timeout=timeout)
                    if body:
                        return body
        print(f'{verify_url} can not get data')

    #检验代理ip是否可用
    def verify_proxy(self, scheme, ip, port, headers, verify_url, timeout):
        proxy = {
            'http':scheme + '://' + ip + ':' + port + '/',
            'https':scheme + '://' + ip + ':' + port + '/'
        }
        try:
            response = requests.get(verify_url, headers=headers, proxies=proxy, timeout=timeout)
            if response.status_code == 200:
                body = response.json()
                body['url'] = verify_url
                return body
        except:
            return None