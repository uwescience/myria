#!/usr/bin/env python

import myriadeploy
import setup_cluster

import subprocess
import sys

def host_port_list(workers):
    return [str(worker[0]) + ':' + str(worker[1]) for worker in workers]

def copy_distribution(config):
    "Copy the distribution (jar and libs and conf) to compute nodes."
    nodes = config['nodes']
    description = config['description']
    default_path = config['path']
    username = config['username']

    for node in nodes:
        (hostname, _, path) = setup_cluster.get_host_port_path(node, default_path)
        if hostname != 'localhost':
            remote_path = "%s@%s:%s/%s-files" % (username, hostname, path, description)
        else:
            remote_path = "%s/%s-files" % (path, description)
        to_copy = ["libs", "conf"]
        args = ["rsync", "-aLvz"] + to_copy + [remote_path]
        if subprocess.call(args):
            raise Exception("Error copying distribution to %s" % (hostname,))

def main(argv):
    # Usage
    if len(argv) != 2:
        print >> sys.stderr, "Usage: %s <deployment.cfg>" % (argv[0])
        print >> sys.stderr, "       deployment.cfg: a configuration file modeled after deployment.cfg.sample"
        sys.exit(1)

    config = myriadeploy.read_config_file(argv[1])

    # Step 1: Copy over libs, "conf", myriad
    copy_distribution(config)

if __name__ == "__main__":
    main(sys.argv)
