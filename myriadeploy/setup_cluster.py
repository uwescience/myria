#!/usr/bin/env python

import ConfigParser
import socket
import subprocess
import sys
import getpass

def host_port_list(workers):
    return map(lambda (x,y) : str(x)+':'+str(y), workers)

def read_workers(filename):
    ret = []
    for line in open(filename,'r'):
        line = line.strip()
        # Skip blank lines or comments
        if len(line) == 0 or line[0] == '#':
            continue

        # Extract the username@host:port string
        wholeline = line.split('@')
        if len(wholeline) == 2:
            username = wholeline[0]
            line = wholeline[1]
        else:
            username = getpass.getuser()

        # Extract the host:port string
        hostline = line.split(':')
        if len(hostline) != 2:
            raise Exception("expected host:port instead of %s" % (line))
        hostname = hostline[0]
        try:
            socket.gethostbyname(hostname)
        except socket.error:
            raise Exception("unable to resolve hostname %s" % (hostname))
        try:
            port = int(hostline[1])
        except:
            raise Exception("unable to convert %s to an int" % (port))
        ret.append((hostname, port, username))
    return ret

def make_catalog(description, master, workers):
    nodes = [master] + workers
    args = ["rm", "-rf", description]
    subprocess.call(args)
    args = ["./run_catalog_maker.sh", \
            description, \
            str(len(nodes))]
    args += host_port_list(nodes)
    if subprocess.call(args):
        print >> sys.stderr, "error making the Catalog"
        sys.exit(1)

def remote_mkdir(hostname, dirname, username):
    args = ["ssh", "%s@%s" % (username, hostname), "mkdir", "-p", dirname]
    return subprocess.call(args)

def copy_master_catalog(hostname, dirname, remote_root, username):
    local_path = "%s/%s" % (dirname, "master.catalog")
    remote_path = "%s@%s:%s/%s-files/%s" % (username, hostname, remote_root, dirname, dirname)
    args = ["scp", local_path, remote_path]
    return subprocess.call(args)

def copy_worker_catalog(hostname, dirname, remote_root, i, username):
    local_path = "%s/worker_%d" % (dirname, i)
    remote_path = "%s@%s:%s/%s-files/%s" % (username, hostname, remote_root, dirname, dirname)
    args = ["scp", "-r", local_path, remote_path]
    return subprocess.call(args)

def copy_catalogs(description, remote_root, master, workers, username):
    # Make directories on master
    (hostname,port) = master
    if remote_mkdir(hostname, "%s/%s-files/%s" \
            % (remote_root, description, description), username):
        raise Exception("Error making directory on master %s" \
                % (hostname,))
    # Copy files to master
    if copy_master_catalog(hostname, description, remote_root, username):
        raise Exception("Error copying master.catalog to %s" % (hostname,))

    for (i,(hostname,port)) in enumerate(workers):
        # Workers are numbered from 1, not 0
        worker_id = i + 1

        # Try and make the directory on the worker
        if remote_mkdir(hostname, "%s/%s-files/%s" \
                % (remote_root, description, description), username):
            raise Exception("Error making directory on worker %d %s" \
                    % (worker_id, hostname))
        # Try and copy the files to the worker
        if copy_worker_catalog(hostname, description, remote_root, worker_id, username):
            raise Exception("Error copying worker.catalog to %s " % (hostname,))

def copy_distribution(nodes, dirname, remote_root, username):
    for (hostname, port) in nodes:
        remote_path = "%s@%s:%s/%s-files" % (username, hostname, remote_root, dirname)
        to_copy = ["myriad-0.1.jar", "sqlite4java-282", "conf"]
        args = ["scp", "-r"] + to_copy + [remote_path]
        if subprocess.call(args):
            raise Exception("Error copying distribution to %s" % (hostname,))

def main(argv):
    # Usage
    if len(argv) != 2:
        print >> sys.stderr, "Usage: %s <deployment.cfg>" % (argv[0])
        print >> sys.stderr, "       deployment.cfg: a configuration file modeled after deployment.cfg.sample"
        sys.exit(1)

    # Parse the configuration
    CONFIG_FILE = argv[1]
    config = ConfigParser.RawConfigParser(allow_no_value=True)
    config.read([CONFIG_FILE])

    DESCRIPTION = config.get('deployment', 'name')
    EXPT_ROOT = config.get('deployment', 'path')
    try:
        USER = config.get('deployment', 'username')
    except:
        USER = getpass.getuser()
    def hostPortKeyToTuple(x):
        return tuple(x[0].split(','))
    MASTER = hostPortKeyToTuple(config.items('master')[0])
    WORKERS = map(hostPortKeyToTuple, config.items('workers'))

    # Step 1: make the Catalog
    make_catalog(DESCRIPTION, MASTER, WORKERS)

    # Step 2: Copy each catalog over
    copy_catalogs(DESCRIPTION, EXPT_ROOT, MASTER, WORKERS, USER)

    # Step 3: Copy over java, libs, myriad
    copy_distribution([MASTER] + WORKERS, DESCRIPTION, EXPT_ROOT, USER)

if __name__ == "__main__":
    main(sys.argv)
