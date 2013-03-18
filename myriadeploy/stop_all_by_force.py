#!/usr/bin/env python

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

def stop_all(workers):
    id = 0
    for (hostname, port, username) in workers:
	if id == 0:
  	    cmd = "ssh %s@%s $'ps aux | grep edu.washington.escience.myriad.daemon.MasterDaemon | grep %s | grep -v grep | awk \\'{print $2}\\''" % (username, hostname, username);
	else:
  	    cmd = "ssh %s@%s $'ps aux | grep edu.washington.escience.myriad.parallel.Worker | grep %s | grep -v grep | awk \\'{print $2}\\''" % (username, hostname, username);
	pids = subprocess.check_output(cmd, shell=True).split('\n');
	for pid in pids:
	    if pid != "":
	        print  "killing %s on %s" % (pid, hostname)
	        cmd = "ssh %s@%s kill %s" % (username, hostname, pid);
	        subprocess.call(cmd, shell=True);
        id = id + 1

def main(argv):
    # Usage
    if len(argv) != 2:
        print >> sys.stderr, "Usage: %s <workers.txt> " % (argv[0])
        print >> sys.stderr, "       workers.txt: a list of host:port strings;"
        print >> sys.stderr, "                    the first entry is the master."
        sys.exit(1)

    # Command-line arguments
    WORKERS_FILE = argv[1]

    # Figure out the master and workers
    workers = read_workers(WORKERS_FILE)

    stop_all(workers)

if __name__ == "__main__":
    main(sys.argv)
