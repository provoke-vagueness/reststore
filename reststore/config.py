import os
import yaml
import tempfile
import hashlib


# Define our defaults.
project = 'reststore'
values = dict(
            files=dict(
                name='files',
                hash_function='md5',
                tune_size=100000000,
                root=tempfile.gettempdir(),
                assert_data_ok=False),
            webapp=dict(
                debug=False,
                quiet=False,
                host='127.0.0.1',
                port=8586,
                server='wsgiref',
                proxy_requests=False,
                ),
            client=dict(
                uri='http://127.0.0.1:8586/'),
        )

def _update_values(new):
    for interface, kwargs in new.items():
        values[interface].update(kwargs)

# Load the system configuration file 
if os.path.exists('/etc/reststore.yaml'):
    with open('/etc/reststore.yaml', 'r') as f:
        _update_values(yaml.load(f))


# Load the user configuration file, update config with its values or initialise 
# a new configuration file if it didn't exist. 
_config_file_path = os.path.join(os.path.expanduser('~'), '.%s.yaml' % project)
if os.path.exists(_config_file_path):
    with open(_config_file_path, 'r') as f:
        _update_values(yaml.load(f))
else:
    with open(_config_file_path, 'w') as f:
        yaml.dump(values, f)


# Update config with the values found in our current env
for interface, kwargs in values.items():
    for key, value in kwargs.items():
        environ_key = ('%s_%s_%s' % (project, interface, key)).upper()
        value_type = type(value)
        kwargs[key] = value_type(os.environ.get(environ_key, value))

    
