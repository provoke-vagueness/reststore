from __future__ import print_function
import sys
import os
from getopt import getopt
try:
    import czipfile as zipfile
except ImportError:
    import zipfile

import reststore
from reststore import config


def command_web():
    from reststore import webapp
    webapp.run()
    return 0

def command_get(FilesClass, hexdigest):
    fs = FilesClass()
    try:
        print(fs[hexdigest])
    except KeyError:
        print("Could not find a file for %s..." % hexdigest, file=sys.stderr)
        return -1
    return 0

def command_read(FilesClass, hexdigest, outfile=sys.stdout):
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
        print("%s: %s" % (hexdigest, filepath))
    return 0

def command_unzip(FilesClass, filepath, password=None, flush_every=1000):
    """Add files from the zip file at filepath"""
    if not zipfile.is_zipfile(filepath):
        raise TypeError("Not a zipfile %s" % filepath)
    fs = FilesClass()
    zf = zipfile.ZipFile(filepath)
    if password is not None:
        zf.setpassword(password)
    datalen = 0
    for i, name in enumerate(zf.namelist()):
        data = zf.read(name, pwd=password)
        datalen += len(data)
        hexdigest = fs.bulk_put(data)
        print("%s: %s" % (hexdigest, name))
        if i % flush_every == 0:
            print("flush %s bytes of data..." % datalen)
            txdatalen = fs.bulk_flush()
            print("sent %s bytes of compressed data" % txdatalen)
    print("flush ...")
    fs.bulk_flush()
       
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

    get [FILE-OPTIONS] [HEXDIGEST]
        Return a filepath to the data behind hexdigest. 

        arguments 
            HEXDIGEST of the data to lookup in reststore.    

    read [FILE-OPTIONS] [HEXDIGEST] > stdout
        Attempt to retrieve a file and write it out to stdout.  A check is
        made in the local reststore first, if the file is in available, an
        attempt to read the file from the web reststore is made. 
    
        arguments 
            HEXDIGEST of the data to lookup in reststore.    

    put [FILE-OPTIONS] FILEPATH(s) 
        Put a file into the reststore.   
    
        arguments 
            Path(s) of files to be loaded into the reststore.

    unzip [OPTIONS FILE-OPTIONS] ZIPFILE 
        Extra files from a zipfile straight into the reststore. 
    
        arguments 
            A path to the zip file to extract into the reststore.

        options
            --password=
                Define a password for unzipping the zip file.
            --flush=1000 
                Number of files to read into memory before flushing through
                to the reststore.

    list [OPTIONS FILE-OPTIONS] 
        list out hexdigests found in the reststore.   
    
        options
            --select=[A:B]
                List all of the hashes between A:B.  Hashes are stored
                chronologically.  0 is the first file inserted, -1 is the last
                file inserted.  i.e. select the last 1000 hexdigests -1001:-1

    len [FILE-OPTIONS]
        print out the number of files stored in the reststore.   
    
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
    --weboff
        This flag forces access to a local repository only.
    --uri=%(client_uri)s
        The uri to the upstream reststore web server.


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
            'password=', 'flush=',
            'select=',
            'weboff',
            ])
    except Exception as exc:
        print("Getopt error: %s" % (exc), file=sys.stderr)
        return -1

    webapp_config = config.values['webapp']
    files_config = config.values['files']
    client_config = config.values['client']
    list_command = dict()
    unzip_command = dict()
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

        elif opt in ['--password']:
            unzip_command['password'] = arg
        elif opt in ['--flush']:
            try:
                unzip_command['flush_every'] = int(arg)
            except ValueError as err:
                print("Failed to convert int for %s" % arg, file=sys.stderr)
                return -1

        elif opt in ['--select']:
            try:
                a, b = arg.split(':')
                if not a:
                    a = 0
                if not b:
                    b = -1
            except Exception as err:
                print("Failed to split select range %s" % arg, file=sys.stderr)
                return -1
            try:
                list_command = dict(select_from = int(a), 
                                    select_to = int(b))
            except ValueError as err:
                print("Failed to convert int for %s" % arg, file=sys.stderr)
                return -1

        elif opt in ['--uri']:
            client_config['uri'] = arg

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
 
    elif command == 'read':
        hexdigest = args[0]
        return command_read(FilesClass, hexdigest)
    
    elif command == 'put':
        filepaths = args
        return command_put(FilesClass, filepaths)

    elif command == 'unzip':
        filepath = args[0]
        return command_unzip(FilesClass, filepath, **unzip_command)
    
    elif command == 'list':
        return command_list(FilesClass, **list_command)
    
    elif command == 'len':
        return command_len(FilesClass)

    else:
        print("%s is not a valid command " % command, file=sys.stderr)
        return -1


entry = lambda :main(sys.argv[1:])
if __name__ == "__main__":
    sys.exit(entry())

