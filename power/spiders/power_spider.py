from scrapy.spider import BaseSpider
from scrapy.http import FormRequest, Request
from scrapy.selector import HtmlXPathSelector
from power.items import PowerItem, AreaItem, CompanyItem, PlanItem
from scrapy import log
from random import shuffle

class PowerSpider(BaseSpider):
    name = "power"
    allowed_domains = ["powerswitch.org.nz"]
    start_urls = ['https://www.powerswitch.org.nz/powerswitch/step_one']
    result_url = None
    
    
    def __init__(self, area_id):
        # self.areas = [x for x in range(int(start), int(stop)+1) if x not in [20, 29, 32, 57, 66, 82, 88]]
        # shuffle(self.areas)
        self.area_id = area_id
        log.start()
    

    def start_requests(self):
        log.msg("STARTING SPIDER WITH NUMBER %s" %self.area_id, log.INFO)
        requests = FormRequest(url="https://www.powerswitch.org.nz/powerswitch/step_one",
                formdata={'profile[region]':str(self.area_id)},
                callback=self.step_two)
        yield requests

        
    def step_two(self, response):
        requests = FormRequest(url=response.url,
                    formdata={'profile[hot_water][]': 'ECL',
                              'profile[heating_main]': 'EL',
                              'profile[cooktop]': 'EG'},
                    callback=self.step_three)
        # log.msg("Got to step 2 with number %s and url %s" % (self.area_id, response.url), log.INFO)
        yield requests
    
    def step_three(self, response):
        hxs = HtmlXPathSelector(response)
        post_form = {'profile[electricity_company]' : '24',
                     'profile[electricity_plan]' : '73458',
                     'profile[fixed_terms]' : '3',
                     'profile[estimate_electricity]' : '0',
                     'profile[electricity_start_date_text]' : '1 July 2012',
                     'profile[electricity_end_date_text]' : '30 June 2013',
                     'profile[electricity_tariffs][]' : '8000'}
        if len(hxs.select('//input[@type="radio"]').extract()) > 0:
            post_form['profile[comparing]'] = 'EG'
            post_form['profile[gas_company]'] = '24'
            post_form['profile[gas_plan]'] = '69322'
            post_form['profile[estimate_gas]'] = '0',
            post_form['profile[gas_start_date_text]'] = '1 July 2012',
            post_form['profile[gas_end_date_text]'] = '30 June 2013',
            post_form['profile[gas_tariffs][]'] = '2000'
            
        requests = FormRequest(url=response.url,
                    formdata=post_form,
                    callback=self.pre_results)
        requests.meta['next'] = 0 #arbitrary number to make sure it hits
        # log.msg("Got to step 3 with number %s and url %s" % (self.area_id, response.url), log.INFO)
        yield requests
        
    def pre_results(self, response):
        hxs = HtmlXPathSelector(response)

        item = AreaItem()
        item['area_id'] = self.area_id
        item['area_name'] = hxs.select('//div[@class="summary-cell"]/p/text()').extract()[0].strip() #"".join([x.strip() for x in hxs.select('//div[@class="summary-cell"]/p/text()').extract()[0].split(" ") if x.strip()])
        self.area_name = item['area_name']
        yield item

        # log.msg("Refining results with number %s and url %s" % (self.area_id, response.url), log.INFO)
        # estimated_use = [x.strip() for x in hxs.select('//div[@class="group clearfix"]/p/text()').extract() if x.strip()]
        values = [int(x) for x in hxs.select('//select[@id="profile_electricity_plan_type"]/option/@value').extract()]
        values.sort()
        index = int(response.meta['next'])
        if index == len(values):
            yield None
        else:
            if not self.result_url:
                self.result_url = response.url
            request = FormRequest(url=self.result_url.replace("results", "refine_results"),
                                                    formdata={'profile[electricity_plan_type]' : str(values[index]),
                                                              'profile[discounts][EP]' : '1',
                                                              'profile[discounts][PP]' : '1'},
                                                    callback=self.step_results,
                                                    dont_filter=True)
            request.meta['next'] = index+1
            yield request
            
    
    
    def step_results(self, response):
        hxs = HtmlXPathSelector(response)
        # log.msg("Got to results with number %s and index: %s and type %s" % (self.area_id, response.meta['next']-1, hxs.select('//select[@id="profile_electricity_plan_type"]/option[@selected="selected"]/text()').extract()[0]), log.INFO)

        # STORE COMPANIES
        companies_list = hxs.select('//td[@class="company_name"]')
        for company in companies_list:
            item = CompanyItem()
            item['company_id'] = company.select('a/@href').extract()[0].split('/')[-1]
            item['company_name'] = company.select('a/text()').extract()[0]
            yield item

        #STORE PLAN NAMES
        plan_list = hxs.select('//td[@class="plan"]')
        for plan in plan_list:
            url = plan.select('a/@href').extract()[0]
            item = PlanItem()
            #item['plan_id'] = url.split('/')[url.split('/').index('plan')+1]
            item['plan_name'] = plan.select('a/text()').extract()[0]
            yield item


        electricity_table = hxs.select('//table[@class="results electricity checkbox_limit"]/tbody/tr')
        if len(electricity_table) > 0:
            for row in electricity_table:
                url = row.select('td[@class="plan"]/a/@href').extract()[0]
                item = PowerItem()
                item['area_id'] = self.area_id
                item['price_last_changed'] = row.select('td[@class="price_last_changed"]/text()').extract()[0].strip()
                plan_id = url.split('/')[url.split('/').index('plan')+1]
                if '?' in plan_id:
                    plan_id = plan_id[:plan_id.index('?')]
                item['plan_id'] = plan_id
                item['plan_category'] = hxs.select('//select[@id="profile_electricity_plan_type"]/option[@selected="selected"]/text()').extract()[0]
                item['estimated_savings'] = row.select('td[@class="your_savings"]/span/text()').extract()[0].replace("$", "").replace(",","")
                disc_text = row.select('td[@class="annual_cost"]/span/text()').extract()[0].strip()
                item['plan_general_discount'] = disc_text[disc_text.index('$')+1:disc_text.index(" ", disc_text.index("$"))]
                request = Request(url=url, callback=self.step_deep_results)
                request.meta['item'] = item
                request.meta['next'] = response.meta['next']
                yield request
                
        gas_table = hxs.select('//table[@class="results gas checkbox_limit"]/tbody/tr')
        if len(gas_table) > 0:
            for row in gas_table:
                url = row.select('td[@class="plan"]/a/@href').extract()[0]
                item = PowerItem()
                item['area_id'] = self.area_id
                plan_id = url.split('/')[url.split('/').index('plan')+1]
                if '?' in plan_id:
                    plan_id = plan_id[:plan_id.index('?')]
                item['plan_id'] = plan_id
                # log.msg(item['plan_id'], log.INFO)
                item['price_last_changed'] = row.select('td[@class="price_last_changed"]/text()').extract()[0].strip()
                item['plan_category'] = hxs.select('//select[@id="profile_electricity_plan_type"]/option[@selected="selected"]/text()').extract()[0]
                item['estimated_savings'] = row.select('td[@class="your_savings"]/span/text()').extract()[0].replace("$", "").replace(",","")
                disc_text = row.select('td[@class="annual_cost"]/span/text()').extract()[0].strip()
                item['plan_general_discount'] = disc_text[disc_text.index('$')+1:disc_text.index(" ", disc_text.index("$"))]
                request = Request(url=url, callback=self.step_deep_results)
                request.meta['item'] = item
                request.meta['next'] = response.meta['next']
                yield request
                
        result_request = Request(url=self.result_url, callback=self.pre_results, dont_filter=True)
        result_request.meta['next'] = response.meta['next']
        yield result_request
        
            
    def step_deep_results(self, response):
        hxs = HtmlXPathSelector(response)
        # log.msg("Got to deep results", log.INFO)
        item = PowerItem()
        item['area_id'] = response.meta['item']['area_id']
        item['plan_category'] = response.meta['item']['plan_category']
        item['plan_id'] = response.meta['item']['plan_id']
        item['price_last_changed'] = response.meta['item']['price_last_changed']
        item['estimated_savings'] = response.meta['item']['estimated_savings']
        item['plan_general_discount'] = response.meta['item']['plan_general_discount']
        item['company'] = hxs.select('//td[@class="column_of_1"]/h3/text()').extract()[0]
        item['price_total'] = hxs.select('//td[@class="plan_total"]/h4/text()').extract()[0].replace("$", "")
        item['plan'] = hxs.select('//td[@class="column_of_1"]/div/p/text()').extract()[0]
        item['tariffs'] = self.find_tarrifs(hxs.select('//td[@class="tariff_detail"]/p'))
        item['plan_type'] = hxs.select('//td[@class="column_of_1"]/div/img/@alt').extract()[0][:2].upper()

        subnode = hxs.select('//table[@class="powerswitch_compare plan_details  one_col"]/tbody/tr')
        #item['price_last_changed'] = self.find_price_last_changed(subnode)
        item['discounts'] = self.find_discount(subnode)
        item['special_conditions'] = self.find_special_conditions(subnode)
        item['rewards'] = self.find_rewards(subnode)
        
        subnode_second = hxs.select('//tbody[@class="collapse_body"]/tr')
        item['bond_required'] = self.find_bonds(subnode_second)
        item['price_plan_reviews'] = self.find_price_plan_reviews(subnode_second)
        item['billing_options'] = self.find_billing_options(subnode_second)
        item['online_services'] = self.find_online_services(subnode_second)
        item['other_products'] = self.find_other_products(subnode_second)
        
        yield item
        
    def find_other_products(self, node):
        for sub in node:
            if 'Other products' in sub.select('th/text()').extract():
                return "".join([x.strip() for x in sub.select('td//text()').extract() if x.strip()]).replace("'",'')
        return 'NA'
        
    def find_online_services(self, node):
        for sub in node:
            if 'Online services' in sub.select('th//text()').extract():
                return "".join([x.strip() for x in sub.select('td//text()').extract() if x.strip()]).replace("'",'')
        return 'NA'
        
    def find_billing_options(self, node):
        for sub in node:
            if 'Billing options' in sub.select('th/text()').extract():
                if len(sub.select('td/text()').extract()) > 0:
                    return sub.select('td/text()').extract()[0].replace("'",'')
        return 'NA'
        
    def find_price_plan_reviews(self, node):
        for sub in node:
            if 'Price plan reviews' in sub.select('th/text()').extract():
                if len(sub.select('td/text()').extract()) > 0:
                    return sub.select('td/text()').extract()[0].replace("'",'')
        return 'NA'
        
    def find_bonds(self, node):
        for sub in node:
            if 'Bond required?' in sub.select('th/text()').extract():
                if len(sub.select('td/text()').extract()) > 0:
                    return sub.select('td/text()').extract()[0].replace("'",'')
        return 'NA'
    
    def find_rewards(self, node):
        for sub in node:
            if 'Rewards' in sub.select('th/text()').extract():
                if len(sub.select('td/text()').extract()) > 0:
                    return sub.select('td/text()').extract()[0].replace("'",'')
        return 'NA'
    
    def find_special_conditions(self, node):
        for sub in node:
            if 'Special conditions' in sub.select('th/text()').extract():
                return ' '.join([x.strip() for x in sub.select('td//text()').extract() if x.strip()]).replace("'",'')
        return 'NA'
    
    def find_price_last_changed(self, node):
        for sub in node:
            if 'Prices last changed' in sub.select('th/text()').extract():
                if len(sub.select('td/text()').extract()) > 0:
                    return sub.select('td/text()').extract()[0]
        return 'NA'
    
    def find_discount(self, node):
        for sub in node:
            if 'Discounts' in sub.select('th/text()').extract():
                return [x.strip() for x in sub.select('td/p/text()').extract()]
        return []
        
        
    def find_tarrifs(self, tariffs):
        out_tariffs = {}
        for sub in tariffs:
            h = sub.select('text()').extract()[0].strip()
            if 'unavailable' in h:
                break
            try:
                price = sub.select('span/text()').extract()[0].replace("$", "").replace(" ", "_").lower()
            except:
                price = sub.select('text()').extract()[1].strip()
            out_tariffs[h] = price
        return out_tariffs
        
        



