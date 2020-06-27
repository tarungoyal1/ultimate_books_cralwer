# -*- coding: utf-8 -*-
import scrapy
import re


class FetchBookUrlsSpider(scrapy.Spider):
    name = 'fetch_book_urls'
    allowed_domains = ['www.amazon.com']
    # start_urls = [
    #     'https://www.amazon.com/s?i=stripbooks&bbn=5&rh=n%3A283155%2Cn%3A5%2Cn%3A4053%2Cp_n_feature_two_browse-bin%3A10806566011&dc&fst=as%3Aoff&qid=1593244740&rnid=5&ref=sr_nr_n_17',
    # ]
    # start_urls = ['https://www.amazon.in/s?rh=n%3A976389031%2Cn%3A%21976390031%2Cn%3A1318158031&page=2&qid=1593170560&ref=lp_1318158031_pg_2']
    # start_urls = ['https://www.amazon.in/s/ref=lp_976389031_nr_n_0?fst=as%3Aoff&rh=n%3A976389031%2Cn%3A%21976390031%2Cn%3A1318158031&bbn=976390031&ie=UTF8&qid=1593169718&rnid=976390031']

    # Bind this spider with it's own separate pipeline (BookUrlPipeline)
    custom_settings = {
        'ITEM_PIPELINES': {
            'amazonindiabooks.pipelines.BookUrlPipeline': 400
        }
    }

    def parse(self, response):

        book_urls = response.xpath("//span[contains(@class, 'a-size-medium')] | //h2")
        genre = ['Computer science', 'Software']
        scraped_urls_list = {'genre':genre, 'urls_list':[]}
        urls_list = []

        pattern = re.compile(r'(.+dp\/[a-zA-Z0-9]+)\/?')

        for url in book_urls:
            url_string = response.urljoin(url.xpath(".//parent::a/@href").get())
            match = re.findall(pattern, url_string)
            if match:
                urls_list.append(match[0])
            else:
                urls_list.append(url_string)
        scraped_urls_list['urls_list'] = urls_list[:-1]
        yield scraped_urls_list

        next_page = response.xpath("//a[@class='pagnNext']/@href | //li[contains(@class, 'a-last')]/a/@href").get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)
