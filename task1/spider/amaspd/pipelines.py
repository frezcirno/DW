# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from scrapy.exceptions import DropItem


class AmaspdPipeline:
    def process_item(self, item, spider):
        if 'title' not in item:
            with open('pidbad.txt', 'a', encoding='utf-8') as f:
                f.write(item['pid'] + '\n')
            raise DropItem(item['pid'] + ' parse failed')
        with open('pidok.txt', 'a', encoding='utf-8') as f:
            f.write(item['pid'] + '\n')
        return item
