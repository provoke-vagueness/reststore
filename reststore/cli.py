from __future__ import print_function
import sys
import os
from getopt import getopt

import reststore
from reststore import config


def command_web():
    from reststore import webapp
    webapp.run()
    return 0

def command_get(FilesClass, hexdigest, outfile=sys.stdout):
    fs = FilesClass()
    try:
        with open(fs[hexdigest], 'rb') as f:
            outfile.write(f.read())
    except KeyError:
        print("Could not find a file for %s..." % hexdigest, file=sys.stderr)
        return -1
    return 0

def command_put(FilesClass, filepaths):
    for filepath in filepaths:
        try:
            with open(filepath, 'rb') as f:
                data = f.read()
        except Exception as exc:
            print("Failed to read file %s - %s" % (filepath, exc), 
                        file=sys.stderr)
            return -1
        fs = FilesClass()
        hexdigest = fs.put(data)
        print("%s: %s > %s" % (hexdigest, filepath, fs[hexdigest]))
    return 0

def command_list(FilesClass, select_from=0, select_to=-1):
    fs = FilesClass()
    for hexdigest in fs.select(select_from, select_to):
        print(hexdigest)
    return 0

def command_len(FilesClass):
    fs = FilesClass()
    print(len(fs))
    return 0

defaults = {}
for interface, kwargs in config.values.items():
    c = {"%s_%s" % (interface, key) : value for key, value in kwargs.items()}
    defaults.update(c)

__help__ = """
NAME reststore - control over the reststore 

SYNOPSIS
    reststore [COMMAND]

Commands:
    
    get [OPTIONS FILE-OPTIONS] [HEXDIGEST] > stdout
        Attempt to retrieve a file and write it out to stdout.  A check is
        made in the local reststore first, if the file is in available, an
        attempt to read the file from the web reststore is made. 
    
        arguments 
            HASH define the hash to read from the reststore.

        options
            --weboff
                This flag forces access to a local repository only.
            --uri=%(client_uri)s
                The uri to the reststore web server.

    put [OPTIONS FILE-OPTIONS] FILEPATH(s) 
        Put a file into the reststore.   
    
        arguments 
            A path to the file to load into the reststore.

        options
            --weboff
                This flag forces access to a local repository only.
            --uri=%(client_uri)s
                The uri to the reststore web server.

    list [OPTIONS FILE-OPTIONS]
        list out hexdigests found in the reststore.   
    
        options
            --from=0
            --to=-1
            --weboff
                This flag forces access to a local repository only.
            --uri=%(client_uri)s
                The uri to the reststore web server.

    len [OPTIONS FILE-OPTIONS]
        print out the number of files stored in the reststore.   
    
        options
            --weboff
                This flag forces access to a local repository only.
            --uri=%(client_uri)s
                The uri to the reststore web server.

    web [OPTIONS FILE-OPTIONS] [[HOST:][PORT]] 
        Run the RESTful web app.
        
        arguments 
            HOST:PORT defaults to %(webapp_host)s:%(webapp_port)s

        options
            --server=%(webapp_server)s
                Choose the server adapter to use.
            --debug=%(webapp_debug)s 
                Run in debug mode.
            --quiet=%(webapp_quiet)s
                Run in quite mode.
            --proxy_requests=%(webapp_proxy_requests)s
                If True, this web app will proxy requests through to 
                the authoritative server defined by the client uri.
            --uri=%(client_uri)s
                This client uri points to the authoritative (or next level
                up) reststore web app.

File options:
    --name=%(files_name)s
        Set the default reststore name (i.e. domain or realm) 
    --hash_function=%(files_hash_function)s
        Set the hash function to be used
    --tune_size=%(files_tune_size)s
        Set the approximate size the reststore may grow up to.
    --root=%(files_root)s
        Set the root for the reststore.
    --assert_data_ok=%(files_assert_data_ok)s
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
            'server=', 'debug=', 'quiet=', 'proxy_requests=',
            'name=', 'hash_function=', 'tune_size=', 'root=', 'assert_data_ok=',
            'uri=', 
            'to=', 'from=',
            'weboff',
            ])
    except Exception as exc:
        print("Getopt error: %s" % (exc), file=sys.stderr)
        return -1

    webapp_config = config.values['webapp']
    files_config = config.values['files']
    client_config = config.values['client']
    list_command = dict()
    FilesClass = reststore.FilesClient
    for opt, arg in opts:
        if opt in ['--server']:
            webapp_config['server'] = arg
        elif opt in ['--quiet']:
            webapp_config['quite'] = arg.lower() != 'false'
        elif opt in ['--debug']:
            webapp_config['debug'] = arg.lower() != 'false'
        elif opt in ['--proxy_requests']:
            webapp_config['proxy_requests'] = arg.lower() != 'false'

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

        elif opt in ['--to']:
            try:
                list_command['select_to'] = int(arg)
            except ValueError:
                print("%s is not a valid int" % arg, file=sys.stderr)
                return -1
        elif opt in ['--from']:
            try:
                list_command['select_from'] = int(arg)
            except ValueError:
                print("%s is not a valid int" % arg, file=sys.stderr)
                return -1

        elif opt in ['--weboff']:
            FilesClass = reststore.Files

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
        return command_get(FilesClass, hexdigest)
    
    elif command == 'put':
        filepaths = args
        return command_put(FilesClass, filepaths)
    
    elif command == 'list':
        return command_list(FilesClass, **list_command)
    
    elif command == 'len':
        return command_len(FilesClass, **list_command)

    else:
        print("%s is not a valid command " % command, file=sys.stderr)
        return -1


entry = lambda :main(sys.argv[1:])
if __name__ == "__main__":
    sys.exit(entry())

