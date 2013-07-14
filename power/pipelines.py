# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/topics/item-pipeline.html

import MySQLdb as mdb
from contextlib import closing
from scrapy import log


class PowerPipeline(object):
    
    def open_mysql(self):
        self.conn = mdb.connect(user='root', passwd='root', db='power', host='127.0.0.1', charset="utf8", use_unicode=True)
        self.conn.autocommit(True)
        
    def close_mysql(self):
        self.conn.close()
    
    def doQuery(self, cursor, query):
        try:
            cursor.execute(query)
        except mdb.Error, e:
            log.msg("Error = %d, %s" % (e.args[0], e.args[1]), log.INFO)
        return self.conn.insert_id() or 0
    
    def queryID(self, cursor, query):
        try:
            cursor.execute(query)
            return cursor.fetchone()[0]
        except mdb.Error, e:
            log.msg("Error = %d, %s" % (e.args[0], e.args[1]), log.INFO)
        return 0
    
    def get_or_insert_area_id(self, cursor, item):
        area_id = self.doQuery(cursor, "INSERT IGNORE INTO area (name) VALUES ('%s')" % item['area'])
        if not area_id:
            area_id = self.queryID(cursor, "SELECT id FROM area WHERE name like '%s'" % item['area'])
        return area_id
    
    def get_or_insert_company_id(self, cursor, company):
        comp_id = self.doQuery(cursor, "INSERT IGNORE INTO company (name) VALUES ('%s')" % company)
        if not comp_id:
            comp_id = self.queryID(cursor, "SELECT id FROM company WHERE name like '%s'" % company)
        return comp_id
    
    def get_or_insert_plan_id(self, cursor, plan):
        plan_id = self.doQuery(cursor, "INSERT IGNORE INTO plan (name) VALUES ('%s')" % plan)
        if not plan_id:
            plan_id = self.queryID(cursor, "SELECT id FROM plan WHERE name like '%s'" % plan)
        return plan_id
    
    def get_or_insert_company_plan_id(self, cursor, company, plan):
        comp_id = self.doQuery(cursor, "INSERT IGNORE INTO company_plan (company_name, plan_name) VALUES ('%s', '%s')" % (company, plan))
        if not comp_id:
            comp_id = self.queryID(cursor, "SELECT id from company_plan WHERE company_name='%s' AND plan_name='%s'" % (company, plan))
        return comp_id
    

    
    def get_or_insert_area_company_plan_id(self, cursor, area_id, comp_id, plan_id):
        out_id = self.doQuery(cursor, "INSERT IGNORE INTO area_company_plan (area_id, company_id, plan_id) VALUES (%d, %d, %d)" % (area_id, comp_id, plan_id))
        if not out_id:
            out_id = self.queryID(cursor, "SELECT id from area_company_plan WHERE area_id=%d AND company_id=%d AND plan_id=%d" % (area_id, comp_id, plan_id))
        return out_id
    
    def insert_plan_info(self, cursor, info_id, item):
        out_id = self.doQuery(cursor, "INSERT INTO plan_general (plan_id, plan_type, price_total, price_last_changed, category) VALUES (%d, '%s', %d, '%s', '%s')" % (info_id, item['plan_type'], int(item['price_total']), item['price_last_changed'], item['plan_category']))
        
    def insert_plan_tariff_info(self, cursor, tariffs, info_id):
        self.doQuery(cursor, "DELETE FROM plan_tariff WHERE plan_id = %d" % info_id)
        for name, price in tariffs.iteritems():
            self.doQuery(cursor, "INSERT INTO plan_tariff (plan_id, tariff_name, tariff_price) VALUES (%d, '%s', '%s')" % (info_id, name, price))
            
    def insert_plan_discount_info(self, cursor, discounts, info_id):
        self.doQuery(cursor, "DELETE FROM plan_discount WHERE plan_id=%d" % info_id)
        if len(discounts) >= 2:
            self.doQuery(cursor, "INSERT INTO plan_discount (plan_id, discount_name, discount_amount) VALUES (%d, '%s', '%s')" % (info_id, discounts[0], discounts[1]))
        
    
    def process_item(self, item, spider):
        self.open_mysql()
        with closing(self.conn.cursor()) as cursor:
            area_id = self.get_or_insert_area_id(cursor, item)                
            comp_id = self.get_or_insert_company_id(cursor, item['company'])
            plan_id = self.get_or_insert_plan_id(cursor, item['plan'])
            info_id = self.get_or_insert_area_company_plan_id(cursor, area_id, comp_id, plan_id)
            self.insert_plan_info(cursor, info_id, item)
            self.insert_plan_tariff_info(cursor, item['tariffs'], info_id)
            self.insert_plan_discount_info(cursor, item['discounts'], info_id)
        self.close_mysql()
        return item
