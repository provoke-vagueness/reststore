from __future__ import print_function
import sys
from getopt import getopt

import filestore
from filestore import config
from filestore import webapp


def command_web():
    webapp.run()
    return 0


def command_get(hexdigest, outfile=sys.stdout):
    fs = filestore.FilesClient()
    try:
        outfile.write(fs[hexdigest])
    except KeyError:
        print("Could not find a file for %s..." % (hexdigest, file=sys.stderr))
        return -1
    return 0


defaults = {}
for interface, kwargs in config.values.items():
    c = {"%s_%s" % (interface, key) : value for key, value in kwargs.items()}
    defaults.update(c)

__help__ = """
NAME filestore - control over the filestore 

SYNOPSIS
    filestore [COMMAND]

Commands:
    
    get [OPTIONS FILE-OPTIONS] [HEXDIGEST] > stdout
        Attempt to retrieve a file for the HEXDIGEST from the local followed 
        by the remote filestore then pipe it out to stdout.
    
        arguments 
            HASH define the hash to read from the filestore

        options
            --uri=%(client_uri)s
                The uri to the filestore web server.

    web [OPTIONS FILE-OPTIONS] [[HOST:][PORT]] 
        Run the RESTful web app.
        
        arguments 
            HOST:PORT defaults to %(webapp_host)s:%(webapp_port)s

        options
            --server=%(webapp_server)s
                choose the server adapter to use.
            --debug defaults to %(webapp_debug)s 
                run in debug mode
            --quiet defaults to %(webapp_quiet)s
                run in quite mode

File options:

    --name=%(files_name)s
        Set the default filestore name (i.e. domain or realm) 
    --hash_func=%(files_hash_function)s
        Set the hash function to be used
    --tune_size=%(files_tune_size)s
        Set the approximate size the filestore may grow up to.
    --root=%(files_root)s
        Set the root for the filestore.
    --assert_data_ok=%(assert_data_ok)s
        Do extra checks when reading and writing data.
  

""" % defaults

def main(args):
    if not args:
        print("No arguments provided" , file=sys.stderr)
        return -1
    if '-h' in args or '--help' in args:
        print(__help__)
        return 0

    command = args.pop(0)

    try:
        opts, args = getopt(args, '', [
            'server=', 'debug=', 'quiet=',
            'name=', 'hash_function=', 'tune_size=', 'root=', 'assert_data_ok=',
            'uri=',
            ])
    except Exception as exc:
        print("Getopt error: %s" % (exc), file=sys.stderr)
        return -1

    webapp_config = config.values['webapp']
    files_config = config.values['files']
    client_config = config.values['client']
    for opt, arg in opts:
        if opt in ['--server']:
            webapp_config['server'] = arg
        elif opt in ['--quiet']:
            webapp_config['quite'] = arg.lower() != 'false'
        elif opt in ['--debug']:
            webapp_config['debug'] = arg.lower() != 'false'

        elif opt in ['--name']:
            files_config['name'] = arg
        elif opt in ['--hash_function']:
            files_config['hash_function'] = arg
        elif opt in ['--tune_size']:
            try:
                files_config['tune_size'] = int(arg)
            except ValueError:
                print("%s is not a valid int" % arg, file=sys.stderr)
                return -1
        elif opt in ['--root']:
            files_config['root'] = arg
        elif opt in ['--assert_data_ok']:
            files_config['assert_data_ok'] = arg.lower() != 'false'

        elif opt in ['--uri']:
            client_config['uri'] = arg

    if command == 'web':
        if args:
            hostport = args[0]
            host = webapp_config['host']
            port = webapp_config['port']
            if ':' in hostport:
                host, p = hostport.split(':')
                # may not have a port value
                if p:
                    port = p
            else:
                port = hostport
            try:
                port = int(port)
            except ValueError:
                print("failed to convert port to int (%s)" % port)
                return -1
            webapp_config['host'] = host
            webapp_config['port'] = port
        return command_web()

    elif command == 'get':
        hexdigest = args[0]
        return command_get(hexdigest)

    else:
        print("%s is not a valid command " % command, file=sys.stderr)
        return -1


entry = lambda :main(sys.argv[1:])
if __name__ == "__main__":
    sys.exit(entry())

