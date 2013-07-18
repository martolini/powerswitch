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
    estimated_savings = Field()
    plan_general_discount = Field()
    special_conditions = Field()
    rewards = Field()
    bond_required = Field()
    price_plan_reviews = Field()
    billing_options = Field()
    online_services = Field()
    other_products = Field()