#!/usr/bin/env python

import types
import sys
import urllib.parse
import random
from webtools import get_charset
from traceback import format_exc
import tornado
from tornado import httpclient, iostream, gen, ioloop
from redis_queue import SpiderQueue

io_loop = ioloop.IOLoop.instance()
httpcli = httpclient.AsyncHTTPClient()

@gen.coroutine
def queue_recover(spider_q):
    yield from spider_q.recover()

@gen.coroutine
def httptasker(spider_q, retry=5):
    while True:
        req, callback = yield from spider_q.get(interval=1.0)
        if req is None:
            tl = random.random()*0.001
            yield gen.Task(io_loop.add_timeout, tl)
            continue
        def get_response():
            for i in range(retry):
                try:
                    rq = httpclient.HTTPRequest(**req)
                    response = yield httpcli.fetch(rq)
                    response.ourl = req['url']
                    return response
                except httpclient.HTTPError as e:
                    print('@HTTPError: %s with URL: %s Retry: %s' % (e.code, req['url'], i+1), file=sys.stderr)
                    if e.code in (403, 404):
                        break
                    continue
                except Exception as e:
                    print('@ErrorProcess: %s\nURL: %s' % (e, req['url']), file=sys.stderr)
                    print('@error_trace_back', format_exc(), file=sys.stderr)
                    break
            return None
        response = yield from get_response()
        try:
            if response is None:
                yield from spider_q.fail(req['url'])
                print('@Failed: %s' % req['url'], file=sys.stderr)
                continue
            else:
                yield from spider_q.ack(req['url'])
            c = get_charset(response, default='gb18030')
            response.ubody = response.body.decode(c, 'ignore')
            response.charset = c
            g = callback(response)
            if isinstance(g, types.GeneratorType):
                yield from g
        except Exception as e:
            print('@ErrorProcess: %s\nURL: %s' % (e, req['url']), file=sys.stderr)
            print('@error_trace_back', format_exc(), file=sys.stderr)


def asyncdo(spider_q, n=3):
    #queue_recover(spider_q)
    tasks = [httptasker(spider_q, 8) for i in range(n)]
    for t in tasks:
        io_loop.run_sync(lambda:t)
    #io_loop.close()

