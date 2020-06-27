# -*- coding: utf-8 -*-
import scrapy
import re
import logging
from pymongo import MongoClient

class FetchBookDetailsSpider(scrapy.Spider):
    name = 'fetch_book_details'
    allowed_domains = ['www.amazon.com']
    # start_urls = ['https://www.amazon.com/Elements-Programming-Interviews-Python-Insiders/dp/1537713949']

    # Bind this spider with it's own separate pipeline (BookUrlPipeline)
    custom_settings = {
        'ITEM_PIPELINES': {
            'amazonindiabooks.pipelines.BookDetailsPipeline': 400
        }
    }

    batch_size = 500

    def get_urls(self):
        url_list = []
        while 1:
            cursor = self.col_bookurls.find(filter={'status': 'pending'}).limit(self.batch_size)
            for item in cursor:
                url_list.append(item['url'])
            yield url_list


    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = cls(crawler, *args, **kwargs)
        crawler.signals.connect(spider.idle_consume, scrapy.signals.spider_idle)
        return spider

    def __init__(self, crawler):
        self.crawler = crawler
        self.client = MongoClient()
        self.db = self.client['amazon_books']
        self.col_bookurls = self.db['amazon_book_urls']

    def start_requests(self):
        urls_batch = self.get_urls()
        urls = next(urls_batch)
        for i in range(self.batch_size):
            yield scrapy.Request(urls.pop(0))

    def idle_consume(self):
        """
        Everytime spider is about to close check our urls
        buffer if we have something left to crawl
        """
        reqs = self.start_requests()
        if not reqs:
            return
        logging.info('Consuming batch')
        for req in reqs:
            # print(req)
            self.crawler.engine.schedule(req, self)
        raise scrapy.exceptions.DontCloseSpider




    def parse(self, response):
        book = {}

        book['book_title'] = response.xpath('normalize-space(//h1/span[1]/text())').get()
        book['book_subtitle'] = response.xpath('normalize-space(//h1/span[2]/text())').get()

        authors_data = []

        pattern_ref = re.compile(r'(.*)\/(ref=.*)?')

        authors = response.xpath("//div[@id='bylineInfo']//span[contains(@class, 'author')]")
        for a in authors:
            author = {}
            author['author_name'] = a.xpath(".//span[@class='a-declarative']/a/text()").get()
            author['author_link'] = self.clean_url(
                response.urljoin(a.xpath(".//span[@class='a-declarative']/a/@href").get()), pattern_ref)
            authors_data.append(author)

        book['authors_data'] = authors_data

        book_rating_avg = response.xpath("//div[@id='averageCustomerReviews']//span[contains(@class, 'a-icon-alt')]/text()").get()
        if book_rating_avg:
            book['book_rating_average'] = book_rating_avg.partition('out')[0].rstrip()

        book_rating_count= response.xpath("//a[@id='acrCustomerReviewLink']/span/text()").get()
        if book_rating_count:
            book['book_rating_count'] = book_rating_count.partition('rating')[0].rstrip()

            # is a bestseller, this will help us scrape futher for bestseller list
        isBestseller = response.xpath("//div[@class='badge-wrapper']")

        book['bestseller_data'] = {}

        if isBestseller:
            book['bestseller_data']["isBestSeller"] = "yes"
            for data in isBestseller:
                book['bestseller_data']["bestseller_link"] = self.clean_url(
                    response.urljoin(data.xpath(".//a/@href").get()), pattern_ref)
                book['bestseller_data']["bestseller_category"] = data.xpath(".//a/@title").get()
                book['bestseller_data']["bestseller_number"] = data.xpath(".//a/i/text()").get()
        else:
            book['bestseller_data']["isBestSeller"] = "no"

        book["book_url"] = response.request.url
        yield book

    def clean_url(self, url_string, pattern):
        match = re.findall(pattern, url_string)
        if match:
            return match[0][0]
        return url_string

