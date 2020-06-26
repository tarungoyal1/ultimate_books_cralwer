# -*- coding: utf-8 -*-
import scrapy
from bs4 import BeautifulSoup


class FetchBookUrlsSpider(scrapy.Spider):
    name = 'fetch_book_urls'
    allowed_domains = ['www.amazon.in']
    # start_urls = ['https://www.amazon.in/s?rh=n%3A976389031%2Cn%3A%21976390031%2Cn%3A1318158031&page=2&qid=1593170560&ref=lp_1318158031_pg_2']
    start_urls = ['https://www.amazon.in/s/ref=lp_976389031_nr_n_0?fst=as%3Aoff&rh=n%3A976389031%2Cn%3A%21976390031%2Cn%3A1318158031&bbn=976390031&ie=UTF8&qid=1593169718&rnid=976390031']

    # Bind this spider with it's own separate pipeline (BookUrlPipeline)
    custom_settings = {
        'ITEM_PIPELINES': {
            'amazonindiabooks.pipelines.BookUrlPipeline': 400
        }
    }

    def parse(self, response):

        book_urls = response.xpath("//span[contains(@class, 'a-size-medium')] | //h2")
        genre = ['Action', 'Adventure']
        scraped_urls_list = {'genre':genre, 'urls_list':[]}
        urls_list = []
        for url in book_urls:
            urls_list.append(response.urljoin(url.xpath(".//parent::a/@href").get()))
        scraped_urls_list['urls_list'] = urls_list[:-1]
        yield scraped_urls_list

        next_page = response.xpath("//a[@class='pagnNext']/@href | //li[contains(@class, 'a-last')]/a/@href").get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)
