from multiprocessing.dummy import Pool
from Proxy import Proxy
import pandas as pd
import logging
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
            # logger.info(f'{num}, {res.status_code}, {url}')
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
    # logger.info('try using proxies...........')
    body = proxy.crawl_proxy(headers, url)
    return body

#爬虫函数
def crawl_product(file_name, url, category1_dict, pages=10):
    with open(file_name, 'w+', encoding='utf-8') as f:
        for category in category1_dict.items():
            category_num = 0
            print(f'*{category[0]} is running...........')
            # logger.info(f'*{category[0]} is running...........')
            category_url = url + f'&category%5B%5D={category[1]}'
            # 翻页
            for page in range(1, pages + 1):
                print(f'**{category[0]} page-{page} is running...........')
                # logger.info(f'**{category[0]} page-{page} is running...........')
                page_url = category_url + f'&page={page}'
                page_res = get_http(page_url)
                json_data = jsonpath.jsonpath(page_res, '$.data')[0]
                for line in json_data:
                    if line != None:
                        f.write(json.dumps(line, ensure_ascii=False))
                        f.write('\n')
                category_num += len(json_data)
                print(f'*{category[0]} is {category_num}>>>>>>>>>>>>>>>>>>>>>')
                # logger.info(f'*{category[0]} is {category_num}>>>>>>>>>>>>>>>>>>>>>')
                if len(json_data) < 5000:
                    break

def crawl_seller(file_name, seller):
    seller_inf_list = []
    seller_fee_list = []
    for num, name in enumerate(set(seller)):
        seller_inf = f'https://api.sidelineswap.com/v1/users/{name}?src=locker'
        seller_fee = f'https://api.sidelineswap.com/v1/users/{name}/feedback?&src=locker&page_size=5000'
        seller_inf_list.append(seller_inf)
        seller_fee_list.append(seller_fee)
    pool = Pool(16)
    inf = pool.map(get_http, seller_inf_list)
    # seller_data = merge_feedback(inf, fee)
    f = open(file_name, 'w+', encoding='utf-8')
    for line in inf:
        if line != None:
            f.write(json.dumps(line, ensure_ascii=False))
            f.write('\n')
    fee = pool.map(get_http, seller_fee_list)
    f2 = open('feedback_'+file_name, 'w+', encoding='utf-8')
    for line in fee:
        if line != None:
            f2.write(json.dumps(line, ensure_ascii=False))
            f2.write('\n')

# def merge_feedback(inf, fee):
#     inf_data = [i['data'] for i in inf if i != None]
#     fee_data = [i for i in fee if i != None]
#     for i in inf_data:
#         for j in fee_data:
#             seller = j['url'].split('/')[-2]
#             if seller == i['username']:
#                 i['feedback_detail'] = j['data']
#     return inf_data

def merge_feedback(seller_data, feedback_data):
    seller_data['user_name'] = seller_data['url'].apply(lambda x: x.split('/')[-1].split('?')[0])
    feedback_data['user_name'] = feedback_data['url'].apply(lambda x: x.split('/')[-2])
    x = seller_data.merge(feedback_data, on='user_name', how='left')
    for i in range(len(x)):
        if i != None:
            x['data_x'][i]['feedback_detail'] = x['data_y'][i]
    x['data_x'].to_json('seller&feedback_quanliang.json', orient='records', lines=True)

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
                         'Other1': '110230',
                         'Scarves': '110870',
                         # 'Clubs': '110430',
                         'Drivers': '110065',
                         'Fairway Woods': '110066',
                         'Hybrids': '110067',
                         'Iron Sets': '110068',
                         'Complete Sets': '110077',
                         'Single Irons': '110081',
                         'Wedges': '110069',
                         'Putters': '110070',
                         'Club Heads': '110432',
                         'Assorted Golf Clubs': '110728',
                         'Shafts': '110078',
                         'Grips': '110079',
                         'Bags Carts': '110437',
                         'Shoes': '110073',
                         'Balls': '110082',
                         'Headcovers': '110435',
                         'On-Course': '110417',
                         'Other2': '110076',
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
                         'Other3': '110173',
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
seller_file = 'seller_quanliang.json'

#日志
# logger = logging.getLogger()
# logger.setLevel(logging.INFO)
# handler=logging.FileHandler("crawl_log.log")
# handler.setLevel(logging.INFO)
# formatter = logging.Formatter('%(asctime)s|%(name)-2s: %(levelname)-2s %(message)s')
# handler.setFormatter(formatter)
# logger.addHandler(handler)

print(f'start crawling, time is {datetime.datetime.today()} >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
crawl_product(product_file, producturl, category_mix_dict, 10)
product_data = pd.read_json(product_file, lines=True)
product_data_noduplicate = product_data.drop_duplicates(['id'])
seller = product_data_noduplicate.seller.apply(lambda x: x['username']).tolist()
crawl_seller(seller_file, seller)
# logger.info('finish>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
print(f'finish crawl, time is {datetime.datetime.today()} >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')

seller_data = pd.read_json(seller_file, lines=True)
feedback_data = pd.read_json('feedback_'+seller_file, lines=True)
merge_feedback(seller_data, feedback_data)
