from multiprocessing.dummy import Pool
from Proxy import Proxy
import pandas as pd
import requests
import jsonpath
import datetime
import random
import json
import time

#request函数
def get_http(url):
    # 防止请求过快
    time.sleep(random.randint(1, 5))
    for num in range(5):
        try:
            res=requests.get(url=url, headers=headers, timeout=100, verify=False)
            res.encoding='utf-8'
            print(num, res.status_code, url)
            if res.status_code == 200:
                body = res.json()
                body['url'] = url
                return body
            elif res.status_code == 404:
                print(f'{url} is 404')
                return None
            time.sleep(120)*num
        except Exception as e:
            time.sleep(120)
    proxy = Proxy(3, headers=headers)
    print('try using proxies...........')
    body = proxy.crawl_proxy(headers, url)
    return body

#爬虫函数
def crawl_product(file_name, url, category1_dict, pages=10):
    f = open(file_name, 'a+', encoding='utf-8')
    diff_count = 0
    # 翻页
    for category in category1_dict.items():
        category_num = 0
        print(f'*{category[0]} is running...........')
        category_url = url + f'&category%5B%5D={category[1]}'
        for page in range(1, pages + 1):
            page_url = category_url + f'&page={page}'
            page_res = get_http(page_url)
            json_data = jsonpath.jsonpath(page_res, '$.data')[0]
            for line in json_data:
                if istoday(line):
                    f.write(json.dumps(line, ensure_ascii=False))
                    f.write('\n')
                else:
                    #可能存在部分数据不按顺序展示的问题
                    diff_count += 1
            category_num += len(json_data)
            print(f'*{category[0]} is {category_num}>>>>>>>>>>>>>>>>>>>>>')
            if len(json_data) < 5000:
                break
            if diff_count > 2:
                break

def crawl_seller(file_name1, file_name2,seller):
    seller_inf_list = []
    seller_fee_list = []
    for num, name in enumerate(set(seller)):
        seller_inf = f'https://api.sidelineswap.com/v1/users/{name}?src=locker'
        seller_fee = f'https://api.sidelineswap.com/v1/users/{name}/feedback?&src=locker&page_size=5000'
        seller_inf_list.append(seller_inf)
        seller_fee_list.append(seller_fee)
    pool = Pool(16)
    inf = pool.map(get_http, seller_inf_list)
    f = open(file_name1, 'w+', encoding='utf-8')
    for line in inf:
        if line != None:
            f.write(json.dumps(line, ensure_ascii=False))
            f.write('\n')
    fee = pool.map(get_http, seller_fee_list)
    f2 = open(file_name2, 'w+', encoding='utf-8')
    for line in fee:
        if line != None:
            f2.write(json.dumps(line, ensure_ascii=False))
            f2.write('\n')

def merge_feedback(seller_data, feedback_data):
    seller_data['user_name'] = seller_data['url'].apply(lambda x: x.split('/')[-1].split('?')[0])
    feedback_data['user_name'] = feedback_data['url'].apply(lambda x: x.split('/')[-2])
    x = seller_data.merge(feedback_data, on='user_name', how='left')
    for i in range(len(x)):
        if i != None:
            x['data_x'][i]['feedback_detail'] = x['data_y'][i]
    x['data_x'].to_json(merge_file, orient='records', lines=True)

#检验时间,应该是去获取前一天
#时区不一致问题
def istoday(line):
    updated_at = datetime.datetime.strptime(line['updated_at'][:-6], \
                                            "%Y-%m-%dT%H:%M:%S.000").date()
    today = datetime.datetime.today().date()
    today -= datetime.timedelta(1)
    return updated_at >= today

#-----------------------------------------------------------------------------------------------------------------------

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

    category1_dict = {'Apparel': '110165',
         'Golf': '7000',
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
product_file = '/home/ubuntu/sidelineswape/updated_data/product_updated.json'
seller_file = '/home/ubuntu/sidelineswape/updated_data/seller_updated.json'
feedback_file = '/home/ubuntu/sidelineswape/updated_data/feedback_seller_updated.json'
merge_file = '/home/ubuntu/sidelineswape/updated_data/seller&feedback_updated.json'


print(f'start crawling, time is {datetime.datetime.today()} >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
crawl_product(product_file, producturl, category1_dict, 10)

product_data = pd.read_json(product_file, lines=True)
product_data['updated_ts'] = product_data.updated_at.apply(lambda x: str(x)[:-15])
today = datetime.datetime.today().date()
today -= datetime.timedelta(1)
product_data_today = product_data[product_data['updated_ts'] == today.strftime("%Y-%m-%d")]
seller = product_data_today.seller.apply(lambda x: x['username']).tolist()
print(f'seller count:{len(set(seller))} >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
crawl_seller(seller_file, feedback_file, seller)
print(f'finish crawl, time is {datetime.datetime.today()} >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')

seller_data = pd.read_json(seller_file, lines=True)
feedback_data = pd.read_json(feedback_file, lines=True)
merge_feedback(seller_data, feedback_data)
print('ok')
print('ok')
print('ok')

