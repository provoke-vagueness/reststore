import bottle
import inspect
import traceback
import functools
import zlib
import base64
import httplib
import json

import filestore
from filestore import config

proxy_requests = False

class JSONError(bottle.HTTPResponse):
    def __init__(self, status, message='', exception='Exception'):
        if inspect.isclass(exception) and issubclass(exception, Exception):
            exception = exception.__name__
        elif isinstance(exception, Exception):
            exception = exception.__class__.__name__
        elif not type(exception) in [str, unicode]:
            raise Exception("unknown exception type %s" % type(exception))
        body = json.dumps({'error': status,
                            'exception': exception,
                            'message': message})
        bottle.HTTPResponse.__init__(self, status=status, 
                header={'content-type':'application/json'}, body=body)
        

def wrap_json_error(f):
    @functools.wraps(f)
    def wrapper(*a, **k):
        try:
            return f(*a, **k)
        except JSONError:
            raise 
        except Exception as exc:
            raise JSONError(httplib.INTERNAL_SERVER_ERROR,
                    exception=exc,
                    message=traceback.format_exc())
    return wrapper

        
@bottle.get('/<name>/file/<hexdigest>')
@wrap_json_error
def get(name, hexdigest):
    if proxy_requests:
        files = filestore.FilesClient(name=name)
    else:
        files = filestore.Files(name=name)
    try:
        filepath = files[hexdigest]
        with open(filepath) as f:
            data = base64.encodestring(zlib.compress(f.read()))
    except KeyError:
        raise JSONError(httplib.NOT_FOUND, 
                exception='KeyError',
                message='%s not found in %s' % (hexdigest, name))
    return dict(result=data)


MAX_FILESIZE = 100 * 2**21
@bottle.put('/<name>/file/<hexdigest>')
@wrap_json_error
def put(name, hexdigest):
    if proxy_requests:
        files = filestore.FilesClient(name=name)
    else:
        files = filestore.Files(name=name)
    data = bottle.request.body.read(MAX_FILESIZE)
    data = zlib.decompress(base64.decodestring(data))
    try:
        files.put(data, hexdigest=hexdigest)
    except ValueError as err:
        raise JSONError(httplib.NOT_FOUND, 
                exception='ValueError',
                message=str(err))
    return dict(result=None)


@bottle.get('/<name>/length')
@wrap_json_error
def get_length(name):
    if proxy_requests:
        files = filestore.FilesClient(name=name)
    else:
        files = filestore.Files(name=name)
    return dict(result=len(files))


@bottle.get('/<name>/select/<a>/<b>')
@wrap_json_error
def get_select(a, b):
    if proxy_requests:
        files = filestore.FilesClient(name=name)
    else:
        files = filestore.Files(name=name)
    hexdigests = files.select(int(a), int(b))
    return dict(result=hexdigests)


@bottle.get('/<name>/contains/<hexdigest>')
@wrap_json_error
def contains(name, hexdigest):
    if proxy_requests:
        files = filestore.FilesClient(name=name)
    else:
        files = filestore.Files(name=name)
    return dict(result=hexdigest in files)


app = bottle.default_app()
def run():
    global proxy_requests
    webapp_config = config.values['webapp']
    bottle_kwargs = dict(debug=webapp_config['debug'],
                       quiet=webapp_config['quiet'],
                       host=webapp_config['host'],
                       port=webapp_config['port'],
                       server=webapp_config['server'])
    proxy_requests = config.values['webapp']['proxy_requests']
    bottle.run(app=app, **bottle_kwargs)



