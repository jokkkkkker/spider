# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class TreetPipeline:
    def process_item(self, item, spider):
        f = open('test123.csv', 'a+', encoding='utf-8')
        f.write(item)
        return item

