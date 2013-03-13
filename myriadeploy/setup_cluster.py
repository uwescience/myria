#!/usr/bin/env python

import socket
import subprocess
import sys

def host_port_list(workers):
    return map(lambda (x,y) : str(x)+':'+str(y), workers)

def read_workers(filename):
    ret = []
    for line in open(filename,'r'):
        line = line.strip()
        # Skip blank lines or comments
        if len(line) == 0 or line[0] == '#':
            continue

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
        ret.append((hostname, port))
    return ret

def make_catalog(description, workers):
    args = ["rm", "-r", description]
    subprocess.call(args);
    args = ["./run_catalog_maker.sh", \
            description, \
            str(len(workers))]
    args += host_port_list(workers)
    if subprocess.call(args):
        print >> sys.stderr, "error making the Catalog"
        sys.exit(1)

def remote_mkdir(hostname, dirname):
    args = ["ssh", hostname, "mkdir", "-p", dirname]
    return subprocess.call(args)

def copy_master_catalog(hostname, dirname, remote_root):
    local_path = "%s/%s" % (dirname, "master.catalog")
    remote_path = "%s:%s/%s-files/%s" % (hostname, remote_root, dirname, dirname)
    args = ["scp", local_path, remote_path]
    return subprocess.call(args)

def copy_worker_catalog(hostname, dirname, remote_root, i):
    local_path = "%s/worker_%d" % (dirname, i)
    remote_path = "%s:%s/%s-files/%s" % (hostname, remote_root, dirname, dirname)
    args = ["scp", "-r", local_path, remote_path]
    return subprocess.call(args)

def copy_catalogs(description, remote_root, workers):
    for (i,(hostname,port)) in enumerate(workers):
        if remote_mkdir(hostname, "%s/%s-files/%s" \
                % (remote_root, description, description)):
            raise Exception("Error making directory on master %s" \
                    % (hostname,))
        # Master
        if i == 0:
            if copy_master_catalog(hostname, description, remote_root):
                raise Exception("Error copying master.catalog to %s" % (hostname,))
        # Workers
        else:
            if copy_worker_catalog(hostname, description, remote_root, i):
                raise Exception("Error copying worker.catalog to %s " % (hostname,))

def copy_distribution(workers, dirname, remote_root):
    for (hostname, port) in workers:
        remote_path = "%s:%s/%s-files" % (hostname, remote_root, dirname)
        to_copy = ["myriad-0.1.jar", "sqlite4java-282",
                   "start_server.py", "start_workers.py",
                   "conf"]
        args = ["scp", "-qr"] + to_copy + [remote_path]
        if subprocess.call(args):
            raise Exception("Error copying distribution to %s" % (hostname,))

def main(argv):
    # Usage
    if len(argv) != 4:
        print >> sys.stderr, "Usage: %s <description> <expt_root> <workers.txt>" % (argv[0])
        print >> sys.stderr, "       description: any alphanumeric plus '-_' string."
        print >> sys.stderr, "       expt_root: where the files should be stored locally, e.g., /scratch."
        print >> sys.stderr, "       workers.txt: a list of host:port strings;"
        print >> sys.stderr, "                    the first entry is the master."
        sys.exit(1)

    # Command-line arguments
    DESCRIPTION = argv[1]
    EXPT_ROOT = argv[2]
    WORKERS_FILE = argv[3]

    # Figure out the master and workers
    workers = read_workers(WORKERS_FILE)

    # Step 1: make the Catalog
    make_catalog(DESCRIPTION, workers)

    # Step 2: Copy each catalog over
    copy_catalogs(DESCRIPTION, EXPT_ROOT, workers)

    # Step 3: Copy over java, libs, myriad
    copy_distribution(workers, DESCRIPTION, EXPT_ROOT)

if __name__ == "__main__":
    main(sys.argv)
