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
