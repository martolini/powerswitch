#!/bin/bash
# @param (int) area_id
cd power
sudo scrapy crawl power -a area_id=$1
exit 0