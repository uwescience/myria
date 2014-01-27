#!/usr/bin/env python

import subprocess
import sys

def host_port_list(workers):
    return [str(worker[0]) + ':' + str(worker[1]) for worker in workers]

def get_host_port_path(node, default_path):
    if len(node) == 2:
        (hostname, port) = node
        if default_path is None:
            raise Exception("Path not specified for node %s" % str(node))
        else:
            path = default_path
    else:
        (hostname, port, path) = node[:3]
    return (hostname, port, path)

def copy_distribution(config_file):
    "Copy the distribution (jar and libs and conf) to compute nodes."
    args = ["./using_deployment_utils.sh", config_file, "-copy_distribution"]
    if subprocess.call(args):
        raise Exception("Error copying distribution")

    for node in nodes:
        (hostname, _, path) = get_host_port_path(node, default_path)
        if hostname != 'localhost':
            remote_path = "%s@%s:%s/%s-files" % (username, hostname, path, description)
        else:
            remote_path = "%s/%s-files" % (path, description)
        to_copy = ["libs", "conf"]
        args = ["rsync", "--del", "-rtLDvz"] + to_copy + [remote_path]
        if subprocess.call(args):
            raise Exception("Error copying distribution to %s" % (hostname,))

def main(argv):
    # Usage
    if len(argv) != 2:
        print >> sys.stderr, "Usage: %s <deployment.cfg>" % (argv[0])
        print >> sys.stderr, \
            "       deployment.cfg: \
            a configuration file modeled after deployment.cfg.sample"
        sys.exit(1)

    # Step 1: Copy over libs, "conf", myria
    copy_distribution(argv[1])

if __name__ == "__main__":
    main(sys.argv)
