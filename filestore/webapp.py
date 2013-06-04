import bottle
import inspect
import traceback
import functools
import zlib

import filestore
from filestore import config


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

        
@bottle.get('/<name>/file/<hexdigest>')
@wrap_json_error
def get(name, hexdigest):
    files = filestore.Files(name=name)
    result = {}
    try:
        filepath = files[hexdigest]
        with open(filepath) as f:
            result['data'] = zlib.compress(f.read())
    except KeyError:
        raise JSONError(httplib.NOT_FOUND, 
                exception='KeyError',
                message='%s not found in %s' % (hexdigest, name))
    return result


DEFAULT_CHUNK_SIZE = 2 * 2**21
@bottle.put('/<name>/file/<hexdigest>')
@wrap_json_error
def put(name, hexdigest):
    files = filestore.Files(name=name)
    data = []
    chunk = bottle.request.body.read(DEFAULT_CHUNK_SIZE)
    while chunk:
        data.append(chunk)
        chunk = bottle.request.body.read(DEFAULT_CHUNK_SIZE)
    data = zlib.decompress("".join(data))
    try:
        files.put(data, expected=hexdigest)
    except ValueError as err:
        raise JSONError(httplib.NOT_FOUND, 
                exception='ValueError',
                message=str(err))
    return {}

@bottle.get('/<name>/length')
@wrap_json_error
def get_length(name):
    files = filestore.Files(name=name)
    return dict(length=len(files))

@bottle.get('/<name>/select/<a>/<b>')
@wrap_json_error
def get_select(a, b):
    files = filestore.Files(name=name)
    hexdigests = files.select(int(a), int(b))
    return dict(hexdigests=hexdigests)

app = bottle.default_app()
def run():
    bottle.run(app=app, **config.values['webapp'])



