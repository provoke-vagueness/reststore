import json 
import requests 
import zlib

import filestore
from filestore import config

class FilesClient(object):
    def __init__(self, name=config.values['files']['name'], 
                       uri=config.values['client']['uri'],
                       requester=requests,
                       ):
        """

        """
        self.requester = requester
        self._files = filestore.Files(name=name)
        self._name = name
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
        return self.request('get', uri)['length']

    _get_default = object()
    def get(self, hexdigest, d=_get_default):
        try:
            return self[hexdigest]
        except KeyError:
            if d != FilesClient._get_default:
                return d
            return None

    def __getitem__(self, hexdigest):
        # try and get the file back locally first
        try:
            return self._files[hexdigest]
        except KeyError:
            pass
        # fetch data from server
        uri = "%s%s/file/%s" % (self._uri, self._name, hexdigest)
        data = zlib.decompress(self.request('get', uri)['data'])
        self._files[hexdigest] = data
        return self._files[hexdigest]

    def __setitem__(self, hexdigest, data):
        self.put(data, hexdigest=hexdigest)
        
    def put(self, data, hexdigest=None):
        self._files[hexdigest] = data
        uri = "%s%s/file/%s" % (self._uri, self._name, hexdigest)
        self.request('put', uri, data=zlib.compress(data))

    def select(self, a, b):
        uri = "%s%s/select/%s/%s" % (self._uri, self._name, a, b)
        hexdigests = self.request('get', uri)['hexdigests']
        return hexdigests

    def __iter__(self):
        i = 0
        hexdigests = self.select(i, i + 10000)
        while hexdigests:
            for hexdigest in hexdigests:
                yield hexdigest
            i += 1
            hexdigests = self.select(i, i + 10000)

            
