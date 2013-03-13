#!/usr/bin/env python

import socket
import subprocess
import sys
import time

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

def stop_all(workers, username):
    id = 0
    for (hostname, port) in workers:
	if id == 0:
  	    cmd = "ssh %s $'ps aux | grep edu.washington.escience.myriad.daemon.MasterDaemon | grep %s | awk \\'{print $2}\\' | head -1'" % (hostname, username);
	    pid = subprocess.check_output(cmd, shell=True);
            pid = pid.strip('\n');
	    print  "killing %s on %s" % (pid, hostname)
	    cmd = "ssh %s kill %s" % (hostname, pid);
	    subprocess.call(cmd, shell=True);
	else:
  	    cmd = "ssh %s $'ps aux | grep edu.washington.escience.myriad.parallel.Worker | grep %s | awk \\'{print $2}\\' | head -1'" % (hostname, username);
	    pid = subprocess.check_output(cmd, shell=True);
            pid = pid.strip('\n');
	    print "killing %s on %s" % (pid, hostname)
	    cmd = "ssh %s kill %s" % (hostname, pid);
	    subprocess.call(cmd, shell=True);
        id = id + 1

def main(argv):
    # Usage
    if len(argv) != 3:
        print >> sys.stderr, "Usage: %s <workers.txt> <user_name>" % (argv[0])
        print >> sys.stderr, "       workers.txt: a list of host:port strings;"
        print >> sys.stderr, "                    the first entry is the master."
        print >> sys.stderr, "       user_name: your username used for running myria processes;"
        sys.exit(1)

    # Command-line arguments
    WORKERS_FILE = argv[1]

    # Figure out the master and workers
    workers = read_workers(WORKERS_FILE)

    stop_all(workers, argv[2])

if __name__ == "__main__":
    main(sys.argv)
