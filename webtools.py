import sys
import re
import cchardet as chardet
from urllib.parse import urlparse, urljoin
import asyncio
from random import random

def req_gen(url, baseurl='', method='GET'):
    url = urljoin(url, baseurl)
    parsed_url = urlparse(url)
    headers = {
        "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language":"en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4",
        "Accept-Encoding": "gzip",
        "Host":parsed_url.hostname,
        "Referer":baseurl,
        "Cache-Control":"no-cache",
        #"Connection":"keep-alive",
        "User-Agent":'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.152 Safari/537.36',
        }
    return {
            'method': method,
            'url': url,
            'headers':headers,
            'connect_timeout': 5,
            'request_timeout': 8,
            'use_gzip': True,
            'user_agent':headers['User-Agent'],
            }


charset_pat = re.compile(r'charset *= *"?([-a-zA-Z0-9]+)', re.IGNORECASE)

def charset_trans(charset):
    if charset.lower() in ('gbk', 'gb2312'):
        return 'gb18030'
    return charset.lower()


def charset_det(body):
    for i in range(3):
        c = chardet.detect(body)['encoding']
        if c is not None:
            return c.lower()
    return None


def get_charset(response, default='utf-8'):
    to_test = response.body.decode('utf-8', 'ignore')
    tmpset = set(charset_trans(e) for e in charset_pat.findall(to_test.lower().split(r'</head>', 1)[0]))
    if len(tmpset) == 1:
        return tmpset.pop()
    elif len(tmpset) > 1:
        c = charset_det(response.body)
        if c is not None:
            return c
    try:
        m = charset_pat.search(response.headers['Content-Type'])
    except:
        m = None
    if m is not None:
        return m.groups()[0]
    print('@Warning: charset detect failed! set to %s by default\nURL: %s\nData: %s' % (default, response.ourl, to_test), file=sys.stderr)
    return default


