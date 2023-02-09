import scrapy
import jsonpath
from treet import items



class TreetspiderSpider(scrapy.Spider):
    name = 'treetSpider'
    # allowed_domains = ['www.treet.shop']
    start_urls = ['https://www.treet.shop/api/treetShopConfig']
    #获取店铺url
    def parse(self, response):
        shop_id_list = jsonpath.jsonpath(response.json(), '$.data..shopId')
        for shop_id in shop_id_list:#testetstestestetstestestetstestestetstest
            shop_url = f'https://{shop_id}.treet.co/?referrer=treet_shop'
            yield scrapy.Request(shop_url, callback=self.parse_shop)
    #获取详情页urls
    def parse_shop(self, response):
        url = response.url
        head_url = url.split('?')[0][:-1]
        detail_url_list = response.xpath(
            "//div[@class='MuiGrid-root MuiGrid-item MuiGrid-grid-xs-6 MuiGrid-grid-sm-4 MuiGrid-grid-md-3 MuiGrid-grid-lg-3']/a/@href").getall()
        next_page = response.xpath("//a[@class='PaginationLinks_next__2IY1v NamedLink_active']/@href").getall()[0] if\
            response.xpath("//a[@class='PaginationLinks_next__2IY1v NamedLink_active']/@href").getall() != [] else None
        for detail_url in detail_url_list:
            detail_url = head_url+detail_url
            yield scrapy.Request(detail_url, callback=self.parse_detail)
        #翻页
        if next_page:
            page_url = head_url + next_page
            yield scrapy.Request(page_url, callback=self.parse_shop)

    #详情页数据
    def parse_detail(self, response):
        item = items.TreetItem()
        item['product'] = response.xpath("//div[@class='ListingPage_infoPanel__1FviR']/h1//text()").getall()
        item['original_price'] = response.xpath("//div[@class='ListingPage_desktopOriginalPriceValue__2lXAH']//text()").getall()
        item['price'] = response.xpath("//div[@class='ListingPage_desktopPriceValue__2FLH3']/h1/text()").getall()
        item['product_detail'] = response.xpath("//div[@class='MuiAccordionDetails-root']//text()").getall()
        item['content'] = response.xpath("//div[@class='ListingPage_infoPanel__1FviR']/p//text()").getall()
        item['url'] = response.url
        yield item










