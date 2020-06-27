# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from pymongo import MongoClient

class BookUrlPipeline:

    def open_spider(self, spider):
        self.client = MongoClient()
        self.db = self.client['amazon_books']
        self.col_bookurls = self.db['amazon_book_urls']

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        pipeline = [
            {"$group": {"_id": 'null', 'values': {"$push": "$url"}}},
            {"$project":
                 {'non_existing':
                      {"$setDifference": [item['urls_list'], "$values"]}
                  }
             }
        ]
        non_exitng_urls = list(self.col_bookurls.aggregate(pipeline))[0]['non_existing']
        # print(non_exitng_urls)
        count = len(non_exitng_urls)
        if count>0:
            if self.col_bookurls.insert_many([{'url': url, 'status': 'pending', 'genre':item['genre']} for url in non_exitng_urls]):
                print('Inserted {} new book urls'.format(count))
                return True
        else:
            return False

class BookDetailsPipeline:

    def open_spider(self, spider):
        self.client = MongoClient()
        self.db = self.client['amazon_books']
        self.col_book_details = self.db['amazon_book_details']
        self.col_book_urls = self.db['amazon_book_urls']

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        if item:
            if not self.col_book_details.find_one({'book_url':item['book_url']}):
                if self.col_book_details.insert_one(item):
                    if self.col_book_urls.update_one({'url': item['book_url']}, {'$set': {'status': 'done'}},upsert=False).modified_count == 1:
                        return "Book details added having url = {}".format(item['book_url'])
            elif self.col_book_urls.update_one({'url': item['book_url']}, {'$set': {'status': 'done'}},upsert=False).modified_count == 1:
                return "Book already scraped having url = {}".format(item['book_url'])