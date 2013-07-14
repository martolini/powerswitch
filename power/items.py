# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/topics/items.html

from scrapy.item import Item, Field
from random import shuffle

class PowerItem(Item):
    number = Field()
    area = Field()
    company = Field()
    price_total = Field()
    tariffs = Field()
    price_last_changed = Field()
    plan = Field()
    plan_category = Field()
    plan_type = Field()
    discounts = Field()
