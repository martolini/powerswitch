# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/topics/item-pipeline.html

import MySQLdb as mdb
from contextlib import closing
from scrapy import log
from power.items import AreaItem, PowerItem, CompanyItem, PlanItem
from power.settings import MYSQL_SETTINGS
import smtplib
from power.settings import EMAIL_SENDER, EMAIL_RECIPIENT, EMAIL_SUBJECT


class ThePipeline(object):
    new_items = []

    def close_spider(self, spider):
        self.sendmail()

    def open_mysql(self):
        self.conn = mdb.connect(user=MYSQL_SETTINGS['user'], 
            passwd=MYSQL_SETTINGS['password'],
            db=MYSQL_SETTINGS['db'],
            host=MYSQL_SETTINGS['host'],
            charset="utf8", 
            use_unicode=True)
        self.conn.autocommit(True)
        
    def close_mysql(self):
        self.conn.close()
    
    def doQuery(self, cursor, query):
        try:
            cursor.execute(query)
        except mdb.Error, e:
            log.msg("Error = %d, %s" % (e.args[0], e.args[1]), log.INFO)
            log.msg("Query = %s" % query, log.INFO)

        return self.conn.insert_id() or 0

    def get_company_id(self, cursor, company):
        cursor.execute("SELECT id FROM company WHERE name like '%s'" % company)
        return cursor.fetchone()[0]

    def get_plan_id(self, cursor, plan):
        cursor.execute("SELECT id FROM plan WHERE name like '%s'" % plan)
        return cursor.fetchone()[0]

    def insert_plan_info(self, cursor, info_id, item):
        self.doQuery(cursor, "INSERT IGNORE INTO plan_general (plan_id, plan_type, price_total, price_last_changed, category, estimated_savings, general_discount, special_conditions, rewards, bond_required, price_plan_reviews, billing_options, online_services, other_products) VALUES (%d, '%s', %d, '%s', '%s', %s, %s, '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (info_id, item['plan_type'], int(item['price_total']), item['price_last_changed'], item['plan_category'], item['estimated_savings'], item['plan_general_discount'], item['special_conditions'], item['rewards'], item['bond_required'], item['price_plan_reviews'], item['billing_options'], item['online_services'], item['other_products']))
        
    def insert_plan_tariff_info(self, cursor, tariffs, info_id):
        self.doQuery(cursor, "DELETE FROM plan_tariff WHERE plan_id = %d" % info_id)
        for name, price in tariffs.iteritems():
            self.doQuery(cursor, "INSERT IGNORE INTO plan_tariff (plan_id, tariff_name, tariff_price) VALUES (%d, '%s', '%s')" % (info_id, name, price))
            
    def insert_plan_discount_info(self, cursor, discounts, info_id):
        self.doQuery(cursor, "DELETE FROM plan_discount WHERE plan_id=%d" % info_id)
        if len(discounts) >= 2:
            self.doQuery(cursor, "INSERT IGNORE INTO plan_discount (plan_id, discount_name, discount_amount) VALUES (%d, '%s', '%s')" % (info_id, discounts[0], discounts[1]))



    def process_item(self, item, spider):
        self.open_mysql()
        with closing(self.conn.cursor()) as cursor:
            if type(item) == AreaItem:
                self.doQuery(cursor, "INSERT IGNORE INTO area (id, name) VALUES (%s, '%s')" % (item['area_id'], item['area_name']))
            if type(item) == CompanyItem:
                self.doQuery(cursor, "INSERT IGNORE INTO company (id, name) VALUES (%s, '%s')" % (item['company_id'], item['company_name']))
            if type(item) == PlanItem:
                self.doQuery(cursor, "INSERT IGNORE INTO plan (name) VALUES ('%s')" % item['plan_name'])
            if type(item) == PowerItem:
                area_id = int(item['area_id'])
                comp_id = int(self.get_company_id(cursor, item['company']))
                plan_id = int(self.get_plan_id(cursor, item['plan']))
                info_id = int(item['plan_id'])

                self.doQuery(cursor, "INSERT IGNORE INTO area_company_plan (id, area_id, company_id, plan_id) VALUES (%s, %s, %d, %d)" % (info_id, area_id, comp_id, plan_id))
                self.insert_plan_info(cursor, info_id, item)
                self.insert_plan_tariff_info(cursor, item['tariffs'], info_id)
                self.insert_plan_discount_info(cursor, item['discounts'], info_id)
                self.new_items.append(item)
        self.close_mysql()
        return item

    def sendmail(self):
        #Sending an email through gmail using Python - Raghuram Reddy
        msg = "The following plans did not exist: "
        for item in self.new_items:
            msg = msg + "/r/n" + item['plan_name']
        #provide gmail user name and password
        username = 'alekfromserver'
        password = 'martinroed'
        headers = ["from: " + EMAIL_SENDER,
               "subject: " + EMAIL_SUBJECT,
               "to: " + EMAIL_RECIPIENT,
               "mime-version: 1.0",
               "content-type: text/html"]
        headers = "\r\n".join(headers)
        # functions to send an email
        server = smtplib.SMTP('smtp.gmail.com:587')
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(username,password)
        server.sendmail(EMAIL_SENDER, EMAIL_RECIPIENT, headers + "\r\n\r\n" + msg)
        server.quit()



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
            log.msg("Query = %s" % query, log.INFO)

        return self.conn.insert_id() or 0
    
    def queryID(self, cursor, query):
        try:
            cursor.execute(query)
            return cursor.fetchone()[0]
        except mdb.Error, e:
            log.msg("Error = %d, %s" % (e.args[0], e.args[1]), log.INFO)
        return 0
    
    def get_or_insert_area_id(self, cursor, item):
        area_id = self.doQuery(cursor, "INSERT IGNORE INTO area (name) VALUES ('%s')" % item['area'].replace("'",''))
        if not area_id:
            area_id = self.queryID(cursor, "SELECT id FROM area WHERE name like '%s'" % item['area'].replace("'",''))
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
        out_id = self.doQuery(cursor, "INSERT INTO plan_general (plan_id, plan_type, price_total, price_last_changed, category, estimated_savings, general_discount, special_conditions, rewards, bond_required, price_plan_reviews, billing_options, online_services, other_products) VALUES (%d, '%s', %d, '%s', '%s', %s, %s, '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (info_id, item['plan_type'], int(item['price_total']), item['price_last_changed'], item['plan_category'], item['estimated_savings'], item['plan_general_discount'], item['special_conditions'], item['rewards'], item['bond_required'], item['price_plan_reviews'], item['billing_options'], item['online_services'], item['other_products']))
        
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
