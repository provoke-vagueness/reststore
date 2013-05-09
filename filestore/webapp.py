import bottle

import filestore


class JSONError(bottle.HTTPResponse):
    def __init__(self, status, message='', exception='Exception'):
        body = json.dumps({'error': status,
                            'exception': exception,
                            'message': message})
        bottle.HTTPResponse.__init__(self, status=status, 
                header={'application':'json'}, body=body)


@bottle.get('/<name>/<filehash>')
def get(name, filehash):
    result = {}
    try:
        files = filestore.Files(name=name)
        filepath = files[filehash]
        with open(filepath) as f:
            result['data'] = f.read()
    except KeyError:
        raise JSONError(httplib.NOT_FOUND, 
                exception='KeyError',
                message='%s not found in %s' % (filehash, name))
    return result


MAX_FILESIZE = 2 * 2**21

@bottle.put('/<name>/<filehash>')
def put(name, filehash):
    data = []
    chunk = bottle.request.body.read(MAX_FILESIZE)
    while chunk:
        data.append(chunk)
        chunk = bottle.request.body.read(MAX_FILESIZE)
    data = "".join(data)
    try:
        files = Files[name]
        files.put(data, expected=filehash)
    except ValueError as err:
        raise JSONError(httplib.NOT_FOUND, 
                exception='ValueError',
                message=str(err))


app = bottle.default_app()

__help__ = """
NAME filestore-webapp - Start the filestore webapp server

SYNOPSIS
    filestore-webapp [OPTIONS]... [HOST:PORT]

DESCRIPTION

arguments 
    HOST:PORT default '127.0.0.1:8586'
        specify the ip and port to bind this server too

options 
    --root=
        root path to the filestore

    --server=
        choose the server adapter to use.

    --debug
        run in debug mode

    --quiet 
        run in quite mode
"""

def main(args):
    try:
        opts, args = getopt(args, 'h',['help',
            'server=', 'debug', 'quiet',
            ])
    except Exception as exc:
        print("Getopt error: %s" % (exc), file=sys.stderr)
        return -1

    bottle_run_kwargs = dict(app=app, port=8585, debug=False)
    for opt, arg in opts:
        if opt in ['-h', '--help']:
            print(__help__)
            return 0
        elif opt in ['--server']:
            bottle_run_kwargs['server'] = arg
        elif opt in ['--quiet']:
            bottle_run_kwargs['quite'] = True
        elif opt in ['--debug']:
            bottle_run_kwargs['debug'] = True

    if args:
        try:
            host, port = args[0].split(':')
        except Exception as exc:
            print("failed to parse IP:PORT (%s)" % args[0])
            return -1
        try:
            port = int(port)
        except ValueError:
            print("failed to convert port to int (%s)" % port)
            return -1
        bottle_run_kwargs['host'] = host
        bottle_run_kwargs['port'] = port

    bottle.run(**bottle_run_kwargs)


entry = lambda :main(sys.argv[1:])
if __name__ == "__main__":
    sys.exit(entry())

