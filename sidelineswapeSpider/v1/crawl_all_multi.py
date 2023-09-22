from proxy_getdata import Proxy_getdata
from multiprocessing.dummy import Pool
import pandas as pd
import requests
import jsonpath
import random
import json
import time

#request函数
def httpget(url):
    # 防止请求过快
    time.sleep(random.randint(1, 5))
    for num in range(5):
        try:
            res=requests.get(url=url, headers=headers, timeout=100, verify=False)
            res.encoding='utf-8'
            print(num, res.status_code)
            if res.status_code == 200:
                body = res.json()
                return body
            time.sleep(120)*num
        except Exception as e:
            time.sleep(120)
    Proxy = Proxy_getdata(3, headers)
    print('try using proxies...........')
    body = Proxy.crawl_proxy(url)
    return body

#爬虫函数
def crawl_product(file_name, url, category1_dict, pages=10):
    with open(file_name, 'w+', encoding='utf-8') as f:
        for category in category1_dict.items():
            category_num = 0
            print(f'*{category[0]} is running...........')
            category_url = url + f'&category%5B%5D={category[1]}'
            # 翻页
            for page in range(1, pages + 1):
                print(f'**{category[0]} page-{page} is running...........')
                page_url = category_url + f'&page={page}'
                page_res = httpget(page_url)
                json_data = jsonpath.jsonpath(page_res, '$.data')[0]
                for line in json_data:
                    f.write(json.dumps(line, ensure_ascii=False))
                    f.write('\n')
                category_num += len(json_data)
                print(f'*{category[0]} is {category_num}>>>>>>>>>>>>>>>>>>>>>')
                if len(json_data) < 5000:
                    break
def crawl_seller(file_name, seller):
    seller_list = []
    with open(file_name, 'w+', encoding='utf-8') as f:
        for num,name in enumerate(set(seller)):
            print(f'crawling {name}, it is {num}.........')
            seller_inf = f'https://api.sidelineswap.com/v1/users/{name}?src=locker'
            seller_fee = f'https://api.sidelineswap.com/v1/users/{name}/feedback?&src=locker&page_size=5000'
            seller_list.append(seller_inf)
            seller_list.append(seller_fee)
            pool = Pool(16)
            seller = pool.map(httpget, seller_list)
            return seller
            seller_inf = jsonpath.jsonpath(inf, '$.data')[0]
            seller_feedback = jsonpath.jsonpath(fee, '$.data')[0]
            seller_inf['feedback_detail'] = seller_feedback
            f.write(json.dumps(seller_inf, ensure_ascii=False))
            f.write('\n')

if __name__ == "__main__":
    headers = {
        'Connection': 'keep-alive',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
    }
    # category1_dict = {'Apparel': '110165',
    #  'Golf': '7000',
    #  'Hockey': '2000',
    #  'Baseball': '4000',
    #  'Footwear': '110231',
    #  'Lacrosse': '1000',
    #  'Skiing': '3000',
    #  'Softball': '10000',
    #  'Football': '6000',
    #  'Memorabilia': '110730',
    #  'Snowboarding': '8000',
    #  'Soccer': '9000',
    #  'Inline, Roller Street Hockey': '110662',
    #  'Tennis Racquet Sports': '110153',
    #  'Bikes': '110000',
    #  'Fitness Training': '110628',
    #  'Basketball': '5000',
    #  'Surf, Wake Water Sports': '110599',
    #  'Other': '110173',
    #  'Disc Golf': '110612',
    #  "Women's Lacrosse": '101003',
    #  'Figure Skating': '110574',
    #  'Electronics, Gaming Esports': '110639',
    #  'Skateboarding': '110626',
    #  'Motocross': '110174',
    #  'Hike Camp': '110475',
    #  'Snowshoe': '110586',
    #  'Field Hockey': '110576',
    #  'Wrestling': '110605',
    #  'Fishing': '110621',
    #  'Paintball': '110655'}

    category_mix_dict = {'Jackets Coats': '110225',
                         'Jerseys': '110219',
                         'Hats': '110224',
                         'Shirts': '110223',
                         'Face Masks Coverings': '110722',
                         'Sweatshirts Hoodies': '110227',
                         'Base Layers Compression': '110220',
                         'Pants': '110221',
                         'Shorts': '110222',
                         'Vests': '110240',
                         'Winter Gloves': '110226',
                         'Socks': '110228',
                         'Sunglasses': '110229',
                         'Swimsuits': '110615',
                         'Backpacks Bags': '110616',
                         'Other': '110230',
                         'Scarves': '110870',
                         'Clubs': '110430',
                         'Shafts': '110078',
                         'Grips': '110079',
                         'Bags Carts': '110437',
                         'Shoes': '110073',
                         'Balls': '110082',
                         'Headcovers': '110435',
                         'On-Course': '110417',
                         'Other': '110076',
                         'Technology': '110446',
                         'Training Aids': '110436',
                         'Hockey': '2000',
                         'Baseball': '4000',
                         'Footwear': '110231',
                         'Lacrosse': '1000',
                         'Skiing': '3000',
                         'Softball': '10000',
                         'Football': '6000',
                         'Memorabilia': '110730',
                         'Snowboarding': '8000',
                         'Soccer': '9000',
                         'Inline, Roller Street Hockey': '110662',
                         'Tennis Racquet Sports': '110153',
                         'Bikes': '110000',
                         'Fitness Training': '110628',
                         'Basketball': '5000',
                         'Surf, Wake Water Sports': '110599',
                         'Other': '110173',
                         'Disc Golf': '110612',
                         "Women's Lacrosse": '101003',
                         'Figure Skating': '110574',
                         'Electronics, Gaming Esports': '110639',
                         'Skateboarding': '110626',
                         'Motocross': '110174',
                         'Hike Camp': '110475',
                         'Snowshoe': '110586',
                         'Field Hockey': '110576',
                         'Wrestling': '110605',
                         'Fishing': '110621',
                         'Paintball': '110655'}
producturl = f'https://api.sidelineswap.com/v2/facet_items?include_model=1&page_size=5000&sort=last_updated'
product_file = 'product_quanliang.json'
seller_file = 'seller_qianliang.json'


crawl_product(product_file, producturl, category_mix_dict, 10)
product_data = pd.read_json(product_file, lines=True)
product_data_noduplicate = product_data.drop_duplicates(['id'])
seller = product_data_noduplicate.seller.apply(lambda x: x['username']).tolist()
crawl_seller(seller_file, seller)