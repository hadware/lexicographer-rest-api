__author__ = 'hadware'
from os.path import dirname

from werkzeug.contrib.cache import FileSystemCache

CACHE_TIMEOUT = 300

cache = FileSystemCache(dirname(__file__))

class cached(object):

    def __init__(self, cache_key, timeout=None):
        self.timeout = timeout or CACHE_TIMEOUT
        self.cache_key = cache_key

    def __call__(self, f):
        def decorator(*args, **kwargs):
            cached_value = cache.get(self.cache_key)
            if cached_value is None:
                cached_value = f(*args, **kwargs)
                cache.set(self.cache_key, cached_value, self.timeout)
            return cached_value

        return decorator


class memoized(object):
    def __init__(self, cache_key, timeout=None):
        self.timeout = timeout or CACHE_TIMEOUT
        self.cache_key = cache_key

    def __call__(self, f):
        def decorator(*args, **kwargs):
            cached_value = cache.get(self.cache_key)
            if cached_value is None:
                cached_value = f(*args, **kwargs)
                cache.set(self.cache_key, cached_value, self.timeout)
            return cached_value

        return decorator