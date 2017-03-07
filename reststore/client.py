import sys
import json 
import requests 
import zlib
import base64
if sys.version_info[0] < 3:
    builtins = __builtins__
else:
    import builtins 

from prometheus_client import Counter, Summary

import reststore
from reststore import config

reststore_cache_counter = Counter(
    'reststore_cache_lookups_total',
    'RestStore client cache lookup counter',
    ['result']
)

reststore_cache_expiry_summary = Summary(
    'reststore_cache_expiry_duration_seconds',
    'Time spent expiring local cache',
)

def expire_cache(f):
    """
    Decorator to expire any candidate files
    from the local instance if caching enabled.
    """
    def wrap(self, *args, **kwargs):
        if self.cache_max_entries > 0 and \
            self.cache_batch_delete > 0 and \
            len(self._files) > self.cache_max_entries:
            with reststore_cache_expiry_summary.time():
                self._files.expire(self.cache_batch_delete)

        return f(self, *args, **kwargs)
    return wrap

class FilesClient(object):
    def __init__(self, name=None, uri=None,
        cache_max_entries=None, cache_batch_delete=None,
        requester=requests):
        """

        """
        name = name or config.values['files']['name']
        uri = uri or config.values['client']['uri']
        self.cache_max_entries = cache_max_entries or \
            config.values['client']['cache_max_entries']
        self.cache_batch_delete = cache_batch_delete or \
            config.values['client']['cache_batch_delete']
        self.requester = requester
        self._files = reststore.Files(name=name)
        self._name = name
        self._bulk_put = []
        if not uri.endswith('/'):
            uri += '/'
        self._uri = uri

    def request(self, rtype, *args, **kwargs):
        func = getattr(self.requester, rtype)
        r = func(*args, **kwargs)
        content_type = r.headers.get('content-type', None)
        if content_type != 'application/json':
            raise Exception(
                    "content-type!=application/json got %s(%s) %s\n'%s'" %\
                        (content_type, r.status_code, r.url, r.text))
        if not r.ok:
            try:
                out = r.json()
            except Exception:
                out = {}
            etype = out.get('exception', 'Exception')
            eclass = getattr(builtins, etype, Exception)
            raise eclass(out.get('message', 'status: %s' % r.status_code))
        try:
            out = r.json()
        except Exception:
            raise Exception("Failed to decode response after a 200 response")
        return out

    def __len__(self):
        uri = "%s%s/length" % (self._uri, self._name)
        return self.request('get', uri)['result']

    def get(self, hexdigest, d=None):
        try:
            return self[hexdigest]
        except KeyError:
            return d

    @expire_cache
    def __getitem__(self, hexdigest):
        # try and get the file back locally first
        try:
            f = self._files[hexdigest]
            reststore_cache_counter.labels('hit').inc()
            return f
        except KeyError:
            reststore_cache_counter.labels('miss').inc()

        # fetch data from server
        uri = "%s%s/file/%s" % (self._uri, self._name, hexdigest)
        data = self.request('get', uri)['result']
        data = zlib.decompress(base64.decodestring(data))
        self._files[hexdigest] = data
        return self._files[hexdigest]

    def __contains__(self, hexdigest):
        uri = "%s%s/contains/%s" % (self._uri, self._name, hexdigest)
        return self.request('get', uri)['result']

    def __setitem__(self, hexdigest, data):
        self.put(data, hexdigest=hexdigest)

    @expire_cache
    def put(self, data, hexdigest=None):
        hexdigest = self._files.put(data, hexdigest=hexdigest)
        if hexdigest in self:
            return hexdigest 
        uri = "%s%s/file/%s" % (self._uri, self._name, hexdigest)
        data = base64.encodestring(zlib.compress(data))
        self.request('put', uri, data=data)
        return hexdigest

    @expire_cache
    def bulk_put(self, data, hexdigest=None):
        hexdigest = self._files.put(data, hexdigest=hexdigest)
        if hexdigest in self:
            return hexdigest
        data = base64.encodestring(zlib.compress(data))
        self._bulk_put.append((hexdigest, data))
        return hexdigest

    def bulk_flush(self):
        if not self._bulk_put:
            return 0 
        uri = "%s%s/file" % (self._uri, self._name)
        body = {'files': self._bulk_put}
        body = json.dumps(body)
        self.request('post', uri, data=body)
        self._bulk_put = []
        return len(body)

    def select(self, a, b):
        uri = "%s%s/select/%s/%s" % (self._uri, self._name, a, b)
        hexdigests = self.request('get', uri)['result']
        return hexdigests

    def __iter__(self):
        i = 0
        step = 100000
        hexdigests = self.select(i, i + step)
        while hexdigests:
            for hexdigest in hexdigests:
                yield hexdigest
            i += step
            hexdigests = self.select(i, i + step)

