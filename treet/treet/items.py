# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class TreetItem(scrapy.Item):
    # define the fields for your item here like:
    product = scrapy.Field()
    original_price = scrapy.Field()
    price = scrapy.Field()
    product_detail = scrapy.Field()
    content = scrapy.Field()
    url = scrapy.Field()
    # pass
