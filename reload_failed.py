#!/usr/bin/env python3

from redis_queue import SpiderQueue

SpiderQueue('ci_url').redo_failed()
