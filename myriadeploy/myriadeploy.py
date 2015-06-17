#!/usr/bin/env python

"A library used to parse Myria deployment configuration files."

import ConfigParser
import getpass
import sys

def read_config_file(filename='deployment.cfg'):
    "reads a Myria deployment configuration file."

    # Return value is a dictionary of keys -> values
    ret = dict()

    # Open the file
    config = ConfigParser.RawConfigParser(allow_no_value=True)
    config.read([filename])

    # Extract values
    # .. description is the name of the configuration
    ret['description'] = config.get('deployment', 'name')
    # .. path is the root path files will be stored on
    try:
        ret['path'] = config.get('deployment', 'path')
    except ConfigParser.NoOptionError:
        ret['path'] = None
    # .. username is the SSH username for remote commands. Default `whoami`
    try:
        ret['username'] = config.get('deployment', 'username')
    except ConfigParser.NoOptionError:
        ret['username'] = getpass.getuser()
    ret['rest_port'] = config.get('deployment', 'rest_port')
    try:
        ret['ssl'] = config.getboolean('deployment', 'ssl')
    except ConfigParser.NoOptionError:
        ret['ssl'] = False

    # Check for custom database port
    try:
        ret['database_port'] = config.get('deployment', 'database_port')
    except ConfigParser.NoOptionError:
        pass

    # Helper function
    def split_hostportpathdbname_key_append_id(hostport):
        "Splits id = host,port,path,db_name into a tuple (host, port, path, db_name, id)."
        fields = hostport[1].split(':')
        id=int(hostport[0])
        host=None
        port=None
        path=None
        db_name=None
        if len(fields) < 2:
          raise Exception('At least host:port should be provided for worker. '+hostport+' given.')
        else:
          host,port=fields[0:2]
          if len(fields)>=3 and fields[2] !='':
            path=fields[2]
          if len(fields)>=4 and fields[3] !='':
            db_name=fields[3]
        return tuple([host,port,path,db_name,id])

    # .. master is the master node of the cluster.
    ret['master'] = split_hostportpathdbname_key_append_id(config.items('master')[0])
    # .. workers is a list of the worker nodes in the cluster.
    ret['workers'] = [split_hostportpathdbname_key_append_id(w) for w in config.items('workers')]
    # .. nodes is master and workers
    ret['nodes'] = [ret['master']] + ret['workers']
    # .. max_heap_size is the Java maximum heap size
    try:
        ret['max_heap_size_gb'] = config.get('runtime', 'max_heap_size_gb')
    except ConfigParser.NoOptionError:
        ret['max_heap_size_gb'] = ''
    # .. min_heap_size is the Java minimum heap size
    try:
        ret['min_heap_size_gb'] = config.get('runtime', 'min_heap_size_gb')
    except ConfigParser.NoOptionError:
        ret['min_heap_size_gb'] = ''
    try:
        ret['admin_password'] = config.get('deployment', 'admin_password')
    except ConfigParser.NoOptionError:
        ret['admin_password'] = ''

    return ret

def get_host_port_path(node, default_path):
    (hostname, port) = node[0:2]
    if node[2] is None:
        if default_path is None:
            raise Exception("Path not specified for node %s" % str(node))
        else:
            path = default_path
    else:
        path = node[2]
    return (hostname, port, path)

def main(argv):
    "simply try and read the passed-in configuration file."
    # Usage
    if len(argv) != 2:
        print >> sys.stderr, "Usage: %s <deployment.cfg>" % (argv[0])
        print >> sys.stderr, "\tdeployment.cfg: a configuration file modeled after deployment.cfg.sample"
        sys.exit(1)

    read_config_file(argv[1])

if __name__ == "__main__":
    main(sys.argv)
