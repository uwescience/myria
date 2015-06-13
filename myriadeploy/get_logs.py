#!/usr/bin/env python

import myriadeploy

import subprocess
import argparse


def mkdir_if_not_exists(description):
    args = ["mkdir", "-p", description]
    return subprocess.call(args)


def get_std_logs_from_worker(hostname, dirname, username,
                             worker_id, description):
    mkdir_if_not_exists(description)
    if hostname == 'localhost':
        uri = "%s/worker_%s_stdout" % (dirname, worker_id)
    else:
        uri = "%s@%s:%s/worker_%s/worker_%s_stdout" % (
            username, hostname, dirname, worker_id, worker_id)
    args = ["scp", uri, "%s/worker_%s_stdout" % (description, worker_id,)]
    return subprocess.call(args)


def get_error_logs_from_worker(hostname, dirname, username,
                               worker_id, description):
    mkdir_if_not_exists(description)
    if hostname == 'localhost':
        uri = "%s/worker_%s_stderr" % (dirname, worker_id)
    else:
        uri = "%s@%s:%s/worker_%s/worker_%s_stderr" % (
            username, hostname, dirname, worker_id, worker_id)
    args = ["scp", uri, "%s/worker_%s_stderr" % (description, worker_id,)]
    return subprocess.call(args)


def get_logs_from_master(hostname, dirname, username, description):
    mkdir_if_not_exists(description)
    if hostname == 'localhost':
        uri = "%s/master/master_stdout" % (dirname)
    else:
        uri = "%s@%s:%s/master/master_stdout" % (username, hostname, dirname)
    args = ["scp", uri, "%s/master_stdout" % (description)]
    return subprocess.call(args)


def get_error_logs_from_master(hostname, dirname, username, description):
    if hostname == 'localhost':
        uri = "%s/master/master_stderr" % (dirname)
    else:
        uri = "%s@%s:%s/master/master_stderr" % (username, hostname, dirname)
    args = ["scp", uri, "%s/master_stderr" % (description)]
    return subprocess.call(args)


def getlog(config_file, from_worker_id=None):
    ''' get configuration'''
    config = myriadeploy.read_config_file(config_file)

    """Copies the master and worker catalogs to the remote hosts."""
    description = config['description']
    default_path = config['path']
    master = config['master']
    workers = config['workers']
    username = config['username']

    # get logs from master
    if from_worker_id is None or from_worker_id == 0:
        (hostname, _, path) = myriadeploy.get_host_port_path(master, default_path)
        if get_logs_from_master(hostname, "%s/%"
           % (path, description), username, description):
            raise Exception("Error on getting logs from master %s"
                            % (hostname,))
        if get_error_logs_from_master(hostname, "%s/%s"
           % (path, description), username, description):
            raise Exception("Error on getting error logs from master %s"
                            % (hostname,))

    for worker in workers:
        worker_id = worker[-1]
        # get logs from workers
        if from_worker_id is None or from_worker_id == worker_id:
            (hostname, _, path) = myriadeploy.get_host_port_path(worker, default_path)
            if get_std_logs_from_worker(hostname, "%s/%s"
               % (path, description), username, worker_id, description):
                raise Exception("Error on getting logs from worker %d %s"
                                % (worker_id, hostname))
            if get_error_logs_from_worker(hostname, "%s/%s"
               % (path, description), username, worker_id, description):
                raise Exception("Error on getting error logs from worker %d %s"
                                % (worker_id, hostname))


def main():
    parser = argparse.ArgumentParser(description='collect logs from workers')
    parser.add_argument("--worker", type=int, help='worker id')
    parser.add_argument(
        "config", metavar='C', type=str, help='configuration file')
    arguments = parser.parse_args()

    if arguments.worker:
        getlog(arguments.config, arguments.worker)
    else:
        getlog(arguments.config)

if __name__ == "__main__":
    main()
