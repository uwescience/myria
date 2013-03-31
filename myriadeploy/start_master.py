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

def start_master(description, root, workers, MAX_MEM):
    for (hostname, port, username) in workers:
    	cmd = "cd %s/%s-files; nohup java -cp myriad-0.1.jar:conf -Djava.library.path=sqlite4java-282 " % (root, description) + MAX_MEM + " edu.washington.escience.myriad.daemon.MasterDaemon %s 0</dev/null 1>master_stdout 2>master_stderr &" % (description);
    	args = ["ssh", "%s@%s" % (username, hostname), cmd];
   	#print args
    	if subprocess.call(args):
        	print >> sys.stderr, "error starting master %s" % (hostname)
        print hostname
	# don't start workers too quickly before master is ready
	time.sleep(0.5)
    	break;

def main(argv):
    # Usage
    if len(argv) < 4:
        print >> sys.stderr, "Usage: %s <description> <expt_root> <workers.txt> <max_heap_size(optional)>" % (argv[0])
        print >> sys.stderr, "       description: any alphanumeric plus '-_' string."
        print >> sys.stderr, "       expt_root: where the files should be stored locally, e.g., /scratch."
        print >> sys.stderr, "       workers.txt: a list of host:port strings;"
        print >> sys.stderr, "                    the first entry is the master."
	print >> sys.stderr, "       max_heap_size(optional): a string -Xmx[val] which will be passed to each jvm. Example: \"-Xmx2g\" (without quote)"
        sys.exit(1)

    # Command-line arguments
    DESCRIPTION = argv[1]
    EXPT_ROOT = argv[2]
    WORKERS_FILE = argv[3]
    MAX_MEM = ""
    if len(argv) > 4:
        MAX_MEM = argv[4]

    # Figure out the master and workers
    workers = read_workers(WORKERS_FILE)

    start_master(DESCRIPTION, EXPT_ROOT, workers, MAX_MEM)

if __name__ == "__main__":
    main(sys.argv)
