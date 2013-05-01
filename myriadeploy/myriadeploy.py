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
    ret['path'] = config.get('deployment', 'path')
    # .. username is the SSH username for remote commands. Default `whoami`
    try:
        ret['username'] = config.get('deployment', 'username')
    except ConfigParser.NoOptionError:
        ret['username'] = getpass.getuser()
    ret['rest_port'] = config.get('deployment', 'rest_port')

    # Helper function
    def split_hostport_key(hostport):
        "Splits host,port into a tuple (host, port)."
        return tuple(hostport[0].split(','))

    # .. master is the master node of the cluster.
    ret['master'] = split_hostport_key(config.items('master')[0])
    # .. workers is a list of the worker nodes in the cluster.
    ret['workers'] = [split_hostport_key(w) for w in config.items('workers')]
    # .. nodes is master and workers
    ret['nodes'] = [ret['master']] + ret['workers']
    # .. max_heap_size is the Java maximum heap size
    try:
        ret['max_heap_size'] = config.get('deployment', 'max_heap_size')
    except ConfigParser.NoOptionError:
        ret['max_heap_size'] = ''

    return ret

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
