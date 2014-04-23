#!/usr/bin/env python3

import redis
import tornadoredis
from urllib.parse import urlparse
from tornado import gen
from random import random
from webtools import req_gen
from time import time


connection_pool = tornadoredis.ConnectionPool()

def redis_client():
    return tornadoredis.Client(connection_pool=connection_pool)

def time_mapper(t):
    '''
    return 0.0 for empty value
    '''
    if nonetest(t) is False:
        return 0.0
    return float(t)

def nonetest(s):
    '''
    return False if s is None
    '''
    if s is None or s == "":
        return False
    return True

import types

def try_return(g):
    if isinstance(g, types.GeneratorType):
        return (yield from g)
    return g

class SpiderQueue(object):
    def __init__(self, name, callback=None, urlfilter=None, key_gen=None):
        self.rd = redis_client()
        self.name = name
        self.queue = self.name+"_queue"
        self.todo = self.name+"_todo"
        self.doing = self.name+"_doing"
        self.failed = self.name+"_failed"
        self.keypool = self.name+"_urlpool"
        self.hostmap = self.name+"_hostmap"
        self.key_gen = key_gen if key_gen is not None else (lambda x: x)
        self.urlfilter = urlfilter if urlfilter is not None else (lambda _: True)
        self.callback = callback if callback is not None else (lambda x: x)

    def get(self, interval=1.0, key_gen=lambda x:x):
        while True:
            todo_l = yield gen.Task(self.rd.llen, self.todo)
            queue_l = yield gen.Task(self.rd.llen, self.queue)
            if todo_l == 0 and queue_l == 0:
                return None, None
            if queue_l == 0 or random() < todo_l / (todo_l+10000):
                url = yield gen.Task(self.rd.lpop, self.todo)
                if url is None or len(url) == 0:
                    return None, None
                urlkey = key_gen(url)
                if (yield from try_return(self.urlfilter(url))) is False:
                    continue
                if nonetest((yield gen.Task(self.rd.hget, self.keypool, urlkey))):
                    continue
                if nonetest((yield gen.Task(self.rd.hget, self.doing, url))):
                    continue
                hostname = urlparse(url).hostname
                t = time_mapper((yield gen.Task(self.rd.hget, self.hostmap, hostname)))
                if time() - t < interval:
                    yield gen.Task(self.rd.rpush, self.todo, url)
                    return None, None
                req = req_gen(url)
                yield gen.Task(self.rd.hset, self.hostmap, hostname, time())
                yield gen.Task(self.rd.hset, self.doing, url, time())
                return req, self.callback
            if (yield gen.Task(self.rd.llen, self.queue)) > 0:
                yield gen.Task(self.rd.rpush, self.todo, (yield gen.Task(self.rd.lpop, self.queue)))

    def ack(self, url):
        urlkey = self.key_gen(url)
        yield gen.Task(self.rd.hset, self.keypool, urlkey, time())
        yield gen.Task(self.rd.hdel, self.doing, url)

    def fail(self, url):
        urlkey = self.key_gen(url)
        yield gen.Task(self.rd.hset, self.failed, url, time())
        yield gen.Task(self.rd.hset, self.keypool, urlkey, time())
        yield gen.Task(self.rd.hdel, self.doing, url)

    def recover(self):
        to_recover = yield gen.Task(self.rd.hkeys, self.doing)
        yield gen.Task(self.rd.delete, self.doing)
        if len(to_recover) > 0:
            yield gen.Task(self.rd.rpush, self.todo, *to_recover)

    def redo_failed(self):
        r = redis.Redis()
        to_recover = r.hkeys(self.failed)
        r.delete(self.failed)
        if len(to_recover) > 0:
            r.rpush(self.todo, *to_recover)
