#!/usr/bin/env python

import myriadeploy

import subprocess
import sys

def remote_rm(hostname, dirname, username):
    if hostname != 'localhost':
        args = ["ssh", "%s@%s" % (username, hostname), "rm", "-rf", dirname]
    else:
        args = ["rm", "-rf", dirname]
    return subprocess.call(args)

def rm_deployment(config):
    """Copies the master and worker catalogs to the remote hosts."""
    description = config['description']
    path = config['path']
    master = config['master']
    workers = config['workers']
    username = config['username']

    # Make directories on master
    (hostname, _) = master
    if remote_rm(hostname, "%s/%s-files" \
            % (path, description), username):
        raise Exception("Error removing directory on master %s" \
                % (hostname,))

    for (i, (hostname, _)) in enumerate(workers):
        # Workers are numbered from 1, not 0
        worker_id = i + 1

        # Make directories on the worker
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
