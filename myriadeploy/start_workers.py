#!/usr/bin/env python

import ConfigParser
import socket
import subprocess
import sys
import time
import getpass

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

def start_workers(description, root, workers, username, MAX_MEM):
    worker_id = 0
    for (hostname, port) in workers:
        worker_id = worker_id + 1
        cmd = "cd %s/%s-files; nohup java -cp myriad-0.1.jar:conf -Djava.library.path=sqlite4java-282 " % (root, description) + MAX_MEM + " edu.washington.escience.myriad.parallel.Worker --workingDir %s/worker_%d 0</dev/null 1>worker_%d_stdout 2>worker_%d_stderr &" % (description, worker_id, worker_id, worker_id)
        args = ["ssh", "%s@%s" % (username, hostname), cmd]
        if subprocess.call(args):
            print >> sys.stderr, "error starting worker %s" % (hostname)
        print hostname

def main(argv):
    # Usage
    if len(argv) < 2 or len(argv) > 3:
        print >> sys.stderr, "Usage: %s <deployment.cfg>" % (argv[0])
        print >> sys.stderr, "       deployment.cfg: a configuration file modeled after deployment.cfg.sample"
        sys.exit(1)

    # Parse the configuration
    CONFIG_FILE = argv[1]
    config = ConfigParser.RawConfigParser(allow_no_value=True)
    config.read([CONFIG_FILE])

    # Extract the parameters
    DESCRIPTION = config.get('deployment', 'name')
    EXPT_ROOT = config.get('deployment', 'path')
    try:
        USER = config.get('deployment', 'username')
    except:
        USER = getpass.getuser()
    MASTER = config.items('master')[0]
    WORKERS = config.items('workers')
    try:
        MAX_MEM = config.get('deployment', 'max_heap_size')
    except:
        MAX_MEM = ""

    # Start the workers
    start_workers(DESCRIPTION, EXPT_ROOT, WORKERS, USER, MAX_MEM)

if __name__ == "__main__":
    main(sys.argv)
