from scrapy.spider import BaseSpider
from scrapy.http import FormRequest, Request
from scrapy.selector import HtmlXPathSelector
from power.items import PowerItem
from scrapy.selector import HtmlXPathSelector
from scrapy import log
from random import shuffle

class PowerSpider(BaseSpider):
    name = "power"
    allowed_domains = ["powerswitch.org.nz"]
    start_urls = ['https://www.powerswitch.org.nz/powerswitch/step_one']
    result_url = None
    
    
    def __init__(self, start, stop):
        self.areas = [x for x in range(int(start), int(stop)+1) if x not in [20, 29, 32, 57, 66, 82, 88]]
        shuffle(self.areas)
        log.start()
    

    def start_requests(self):
        for number in self.areas:
            requests = FormRequest(url="https://www.powerswitch.org.nz/powerswitch/step_one",
                    formdata={'profile[region]':str(number)},
                    callback=self.step_two)
            item = PowerItem()
            item['number'] = number
            requests.meta['item'] = item
            yield requests

        
    def step_two(self, response):
        requests = FormRequest(url=response.url,
                    formdata={'profile[hot_water][]': 'ECL',
                              'profile[heating_main]': 'EL',
                              'profile[cooktop]': 'EG'},
                    callback=self.step_three)
        requests.meta['item'] = response.meta['item']
        log.msg("Got to step 2 with number %d and url %s" % (requests.meta['item']['number'], response.url), log.INFO)
        yield requests
    
    def step_three(self, response):
        hxs = HtmlXPathSelector(response)
        post_form = {'profile[electricity_company]' : '46',
                     'profile[electricity_plan]' : '76293',
                     'profile[fixed_terms]' : '3',
                     'profile[estimate_electricity]' : '0',
                     'profile[electricity_start_date_text]' : '1 July 2012',
                     'profile[electricity_end_date_text]' : '30 June 2013',
                     'profile[electricity_tariffs][]' : '8000'}
        if len(hxs.select('//input[@type="radio"]').extract()) > 0:
            post_form['profile[comparing]'] = 'EG'
            post_form['profile[gas_company]'] = '1'
            post_form['profile[gas_plan]'] = '52576'
            post_form['profile[estimate_gas]'] = '0',
            post_form['profile[gas_start_date_text]'] = '1 July 2012',
            post_form['profile[gas_end_date_text]'] = '30 June 2013',
            post_form['profile[gas_tariffs][]'] = '2000'
            
        requests = FormRequest(url=response.url,
                    formdata=post_form,
                    callback=self.pre_results)
        requests.meta['item'] = response.meta['item']
        requests.meta['next'] = 0 #arbitrary number to make sure it hits
        log.msg("Got to step 3 with number %d and url %s" % (requests.meta['item']['number'], response.url), log.INFO)
        yield requests
        
    def pre_results(self, response):
        hxs = HtmlXPathSelector(response)
        log.msg("Refining results with number %d and url %s" % (response.meta['item']['number'], response.url), log.INFO)
        estimated_use = [x.strip() for x in hxs.select('//div[@class="group clearfix"]/p/text()').extract() if x.strip()]
        log.msg(" ".join(estimated_use), log.INFO)
        values = [int(x) for x in hxs.select('//select[@id="profile_electricity_plan_type"]/option/@value').extract()]
        values.sort()
        index = int(response.meta['next'])
        if index == len(values):
            return None
        if not self.result_url:
            self.result_url = response.url
        request = FormRequest(url=self.result_url.replace("results", "refine_results"),
                                                formdata={'profile[electricity_plan_type]' : str(values[index]),
                                                          'profile[discounts][EP]' : '1',
                                                          'profile[discounts][PP]' : '1'},
                                                callback=self.step_results,
                                                dont_filter=True)
        request.meta['item'] = response.meta['item']
        request.meta['next'] = index+1
        return request
            
    
    
    def step_results(self, response):
        item = response.meta['item']
        hxs = HtmlXPathSelector(response)
        log.msg("Got to results with number %d and index: %s and shit %s" % (item['number'], response.meta['next']-1, hxs.select('//select[@id="profile_electricity_plan_type"]/option[@selected="selected"]/text()').extract()[0]), log.INFO)
        area = hxs.select('//div[@class="summary-cell"]/p/text()').extract()[0].strip()
        for u in hxs.select('//td[@class="plan"]/a/@href').extract():
            request = Request(url=u, callback=self.step_deep_results)
            item['area'] = area
            item['plan_category'] = hxs.select('//select[@id="profile_electricity_plan_type"]/option[@selected="selected"]/text()').extract()[0]
            request.meta['item'] = item
            request.meta['next'] = response.meta['next']
            yield request
        result_request = Request(url=self.result_url, callback=self.pre_results, dont_filter=True)
        result_request.meta['next'] = response.meta['next']
        result_request.meta['item'] = response.meta['item']
        yield result_request
        
            
    def step_deep_results(self, response):
        hxs = HtmlXPathSelector(response)
        log.msg("Got to deep results", log.INFO)
        item = PowerItem()
        item['plan_category'] = response.meta['item']['plan_category']
        item['area'] = response.meta['item']['area']
        item['company'] = hxs.select('//td[@class="column_of_1"]/h3/text()').extract()[0]
        item['price_total'] = hxs.select('//td[@class="plan_total"]/h4/text()').extract()[0].replace("$", "")
        item['plan'] = hxs.select('//td[@class="column_of_1"]/div/p/text()').extract()[0]
        item['tariffs'] = self.find_tarrifs(hxs.select('//td[@class="tariff_detail"]/p'))
        item['plan_type'] = hxs.select('//td[@class="column_of_1"]/div/img/@alt').extract()[0][:2].upper()

        subnode = hxs.select('//table[@class="powerswitch_compare plan_details  one_col"]/tbody/tr')
        item['price_last_changed'] = self.find_price_last_changed(subnode)
        item['discounts'] = self.find_discount(subnode)
        yield item
        
        
    def find_price_last_changed(self, node):
        for sub in node:
            if 'Prices last changed' in sub.select('th/text()').extract():
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
        
        



