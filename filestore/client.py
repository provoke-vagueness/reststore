import json 
import requests 

from filestore import files 
from filestore import config

class Files(object):
    def __init__(self, name=config.values['files']['name'],
                       uri=config.values['client']['uri'],
                       keep_local=config.values['client']['keep_local'],
                       requester=requests,
                       ):
        self._name = name
        self._keep_local = keep_local
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
    def get(self, hexdigest, d=self._get_default):
        try:
            return self[hexdigest]
        except KeyError:
            if d != self._get_default:
                return d
            raise

    def __getitem__(self, hexdigest):
        # try and get the file back locally first
        try:
            fs = files.Files(name=self._name)
            return fs[hexdigest]
        except KeyError:
            pass
        uri = "%s%s/" % (self._uri, self._name, hexdigest)
        return self.request('get', uri)['data']

        

    
    

