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
        (hostname, port, path) = node
    return (hostname, port, path)

def get_logs_from_worker(hostname, dirname, username, worker_id):
    print hostname
    args = ["scp", "%s@%s:%s/worker_%s_stdout" % (username, hostname, dirname,worker_id), "./worker_%s_stdout" % (worker_id,)]
    return subprocess.call(args)

def get_error_logs_from_worker(hostname, dirname, username, worker_id):
    print hostname
    args = ["scp", "%s@%s:%s/worker_%s_stderr" % (username, hostname, dirname,worker_id), "./worker_%s_stderr" % (worker_id,)]
    return subprocess.call(args)

def get_logs_from_master(hostname, dirname, username):
    print hostname
    args = ["scp", "%s@%s:%s/master_stdout" % (username, hostname, dirname), "./master_stdout" ]
    return subprocess.call(args)

def get_error_logs_from_master(hostname, dirname, username):
    print hostname
    args = ["scp", "%s@%s:%s/master_stderr" % (username, hostname, dirname), "./master_stderr" ]
    return subprocess.call(args)


def getlog(config):
    """Copies the master and worker catalogs to the remote hosts."""
    description = config['description']
    default_path = config['path']
    master = config['master']
    workers = config['workers']
    username = config['username']

    # get logs from master
    (hostname, _, path) = get_host_port_path(master, default_path)
    if get_logs_from_master(hostname, "%s/%s-files" \
            % (path, description), username):
        raise Exception("Error on getting logs from master %s" \
                % (hostname,))

    if get_error_logs_from_master(hostname, "%s/%s-files" \
            % (path, description), username):
        raise Exception("Error on getting error logs from master %s" \
                % (hostname,))

    for (i, worker) in enumerate(workers):
        # Workers are numbered from 1, not 0
        worker_id = i + 1

        # get logs from workers
        (hostname, _, path) = get_host_port_path(worker, default_path)
        if get_logs_from_worker(hostname, "%s/%s-files" \
                % (path, description), username, worker_id):
            raise Exception("Error on getting logs from worker %d %s" \
                    % (worker_id, hostname))
        if get_error_logs_from_worker(hostname, "%s/%s-files" \
                % (path, description), username, worker_id):
            raise Exception("Error on getting error logs from worker %d %s" \
                    % (worker_id, hostname))    


def main(argv):
    # Usage
    if len(argv) != 2:
        print >> sys.stderr, "Usage: %s <deployment.cfg>" % (argv[0])
        print >> sys.stderr, "       deployment.cfg: a configuration file modeled after deployment.cfg.sample"
        sys.exit(1)

    config = myriadeploy.read_config_file(argv[1])
    getlog(config)

if __name__ == "__main__":
    main(sys.argv)
