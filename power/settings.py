# Scrapy settings for power project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/topics/settings.html
#

BOT_NAME = 'power'

SPIDER_MODULES = ['power.spiders']
NEWSPIDER_MODULE = 'power.spiders'
LOG_FILE = 'log.txt'
LOG_ENABLED = True
LOG_LEVEL = 'INFO'
CONCURRENT_REQUESTS = 100
CONCURRENT_REQUESTS_PER_DOMAIN = 100
DOWNLOAD_DELAY = 1.5

ITEM_PIPELINES = ('power.pipelines.ThePipeline')

MYSQL_SETTINGS = {
	'user': 'root',
	'password': 'root',
	'host': '127.0.0.1',
	'db': 'power',
}

EMAIL_SENDER = "alekfromserver@gmail.com"
EMAIL_RECIPIENT = "martin@glide.cc"
EMAIL_SUBJECT = "Update from server"



# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'power (+http://www.yourdomain.com)'
