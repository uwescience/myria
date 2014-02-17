#!/usr/bin/env python

import myriadeploy

import subprocess
import sys

def get_host_port_path(node, default_path):
    if len(node) == 2:
        (hostname, port) = node
        if default_path is None:
            raise Exception("Path not specified for node %s" % str(node))
        else:
            path = default_path
    else:
        (hostname, port, path, _) = node
    return (hostname, port, path)

def remote_rm(hostname, dirname, username):
    print hostname
    if hostname != 'localhost':
        args = ["ssh", "%s@%s" % (username, hostname), "rm", "-rf", dirname]
    else:
        args = ["rm", "-rf", dirname]
    return subprocess.call(args)

def rm_deployment(config):
    """Copies the master and worker catalogs to the remote hosts."""
    description = config['description']
    default_path = config['path']
    master = config['master']
    workers = config['workers']
    username = config['username']

    # Remove directories on master
    (hostname, _, path) = get_host_port_path(master, default_path)
    if remote_rm(hostname, "%s/%s-files" \
            % (path, description), username):
        raise Exception("Error removing directory on master %s" \
                % (hostname,))

    for (i, worker) in enumerate(workers):
        # Workers are numbered from 1, not 0
        worker_id = i + 1

        # Remove directories on the worker
        (hostname, _, path) = get_host_port_path(worker, default_path)
        if remote_rm(hostname, "%s/%s-files" \
                % (path, description), username):
            raise Exception("Error removing directory on worker %d %s" \
                    % (worker_id, hostname))

def main(argv):
    # Usage
    if len(argv) != 2:
        print >> sys.stderr, "Usage: %s <deployment.cfg>" % (argv[0])
        print >> sys.stderr, "       deployment.cfg: a configuration file modeled after deployment.cfg.sample"
        sys.exit(1)

    config = myriadeploy.read_config_file(argv[1])
    rm_deployment(config)

if __name__ == "__main__":
    main(sys.argv)
