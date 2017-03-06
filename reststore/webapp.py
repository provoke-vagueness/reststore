import inspect
import traceback
import functools
import zlib
import base64
import client
import json
import sys
try:
    import httplib as client
except ImportError:
    from http import client

import bottle
from bottle import response
bottle.BaseRequest.MEMFILE_MAX = 1600000000

import prometheus_client
from prometheus_client import Gauge, Summary

import reststore
from reststore import config

proxy_requests = False


# prometheus metrics state
request_summary = Summary(
    'reststore_api_request_duration_seconds',
    'Time spent processing api request',
    ['resource', 'method']
)
request_timer = lambda *x: request_summary.labels(*x).time()

file_count_gauge = Gauge(
    'reststore_stored_files',
    'Number of files in reststore',
    ['store']
)
file_count_gauge._samples = lambda: _counts()

file_size_summary = Summary(
    'reststore_file_size_bytes',
    'Size of files stored/fetched in bytes',
    ['store', 'direction']
)

# unfortunately do not have a way to query for current
# filestores, so we accumulate them as requests are seen
known_file_stores = set()

def _counts():
    return [('', {'store': k}, len(_get_files(k))) \
        for k in known_file_stores]

def _get_files(name):
    global proxy_requests
    known_file_stores.add(name)
    if proxy_requests:
        files = reststore.FilesClient(name=name)
    else:
        files = reststore.Files(name=name)
    return files


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
                content_type='application/json', body=body)
        

def wrap_json_error(f):
    @functools.wraps(f)
    def wrapper(*a, **k):
        try:
            return f(*a, **k)
        except JSONError:
            raise 
        except Exception as exc:
            raise JSONError(client.INTERNAL_SERVER_ERROR,
                    exception=exc,
                    message=traceback.format_exc())
    return wrapper

        
@bottle.get('/<name>/file/<hexdigest>')
@wrap_json_error
@request_timer('/store/file', 'get')
def get(name, hexdigest):
    files = _get_files(name)
    try:
        filepath = files[hexdigest]
        with open(filepath) as f:
            data = f.read()
            file_size_summary.labels(name, 'download').observe(len(data))
            data = base64.encodestring(zlib.compress(data))
    except KeyError:
        raise JSONError(client.NOT_FOUND, 
                exception='KeyError',
                message='%s not found in %s' % (hexdigest, name))
    return dict(result=data)


@bottle.put('/<name>/file/<hexdigest>')
@wrap_json_error
@request_timer('/store/file', 'put')
def put(name, hexdigest):
    files = _get_files(name)
    data = bottle.request.body.read()
    data = zlib.decompress(base64.decodestring(data))
    file_size_summary.labels(name, 'upload').observe(len(data))
    try:
        files.put(data, hexdigest=hexdigest)
    except ValueError as err:
        raise JSONError(client.NOT_FOUND, 
                exception='ValueError',
                message=str(err))
    return dict(result=None)


@bottle.post('/<name>/file')
@wrap_json_error
@request_timer('/store/file', 'post')
def post_multiple_files(name):
    """Multiple file post

    body contains {hexdigest=bash64<zlib<data>>>, ...}
    """
    files = _get_files(name)
    try:
        body = json.loads(bottle.request.body.read())
        insert_files = body['files']
        for hexdigest, data in insert_files:
            data = zlib.decompress(base64.decodestring(data))
            file_size_summary.labels(name, 'upload').observe(len(data))
            files.put(data, hexdigest=hexdigest)
    except ValueError as err:
        raise JSONError(client.BAD_REQUEST,
                        exception='ValueError',
                        message=str(err))
    return {}


@bottle.get('/<name>/length')
@wrap_json_error
@request_timer('/store/length', 'get')
def get_length(name):
    files = _get_files(name)
    return dict(result=len(files))


@bottle.get('/<name>/select/<a>/<b>')
@wrap_json_error
@request_timer('/store/select', 'get')
def get_select(name, a, b):
    files = _get_files(name)
    hexdigests = files.select(int(a), int(b))
    return dict(result=hexdigests)


@bottle.get('/<name>/contains/<hexdigest>')
@request_timer('/store/contains', 'get')
@wrap_json_error
def contains(name, hexdigest):
    files = _get_files(name)
    return dict(result=hexdigest in files)

@bottle.get('/metrics')
@request_timer('/metrics', 'get')
def prometheus_metrics():
    """Prometheus exporter api"""
    response.content_type = prometheus_client.CONTENT_TYPE_LATEST
    return prometheus_client.generate_latest()


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

