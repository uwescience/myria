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

def start_master(description, root, master, username, MAX_MEM):
    (hostname, port) = master
    cmd = "cd %s/%s-files; nohup java -cp myriad-0.1.jar:conf -Djava.library.path=sqlite4java-282 " % (root, description) + MAX_MEM + " edu.washington.escience.myriad.daemon.MasterDaemon %s 0</dev/null 1>master_stdout 2>master_stderr &" % (description,)
    args = ["ssh", "%s@%s" % (username, hostname), cmd]
    if subprocess.call(args):
        print >> sys.stderr, "error starting master %s" % (hostname)
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
    try:
        MAX_MEM = config.get('deployment', 'max_heap_size')
    except:
        MAX_MEM = ""

    start_master(DESCRIPTION, EXPT_ROOT, MASTER, USER, MAX_MEM)

if __name__ == "__main__":
    main(sys.argv)
