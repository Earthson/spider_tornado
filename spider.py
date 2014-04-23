#!/usr/bin/env python

'''
read url task from redis queue: ci_url_queue
fetch html into mongodb: ci.url_html : {'_id':url, 'html':html}
redis queue: doc_todo for incremental html parsing
'''

import sys
from httper import asyncdo
from webtools import req_gen 
from redis_queue import SpiderQueue, redis_client
from urllib import parse
import motor
from tornado import gen

r_cli = redis_client()
mongo = motor.MotorClient().open_sync()


def urlfilter(url):
    if url.split('?', 1)[0][-4:].lower() == '.swf':
        return False
    if 'doubleclick' in url.lower():
        return False
    if 't.rc.yoyi.com.cn' in url.lower():
        return False
    tmp = yield motor.Op(mongo.ci.url_html.find_one, {'_id':url})
    if tmp is not None:
        return False
    return True


def response_do(response):
    yield gen.Task(r_cli.rpush, 'doc_todo', response.ourl)
    yield gen.Task(mongo.ci.url_html.insert, {'_id':response.ourl, 'html':response.ubody})

spider_q = SpiderQueue('ci_url', response_do, urlfilter=urlfilter)

asyncdo(spider_q, n=50)
