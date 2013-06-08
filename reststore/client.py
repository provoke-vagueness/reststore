import sys
import json 
import requests 
import zlib
import base64
if sys.version_info[0] < 3:
    builtins = __builtins__
else:
    import builtins 

import reststore
from reststore import config

class FilesClient(object):
    def __init__(self, name=None, uri=None, requester=requests):
        """

        """
        name = name or config.values['files']['name']
        uri = uri or config.values['client']['uri']
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

    def __getitem__(self, hexdigest):
        # try and get the file back locally first
        try:
            return self._files[hexdigest]
        except KeyError:
            pass
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
        
    def put(self, data, hexdigest=None):
        hexdigest = self._files.put(data, hexdigest=hexdigest)
        if hexdigest in self:
            return hexdigest 
        uri = "%s%s/file/%s" % (self._uri, self._name, hexdigest)
        data = base64.encodestring(zlib.compress(data))
        self.request('put', uri, data=data)
        return hexdigest

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

            
