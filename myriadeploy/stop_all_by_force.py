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

def stop_all(master, workers, username):
    # Stop the Master
    (hostname, port) = master
    cmd = "ssh %s@%s $'ps aux | grep edu.washington.escience.myriad.daemon.MasterDaemon | grep %s | grep -v grep | awk \\'{print $2}\\''" % (username, hostname, username);
    pids = subprocess.check_output(cmd, shell=True).split('\n');
    for pid in pids:
        if pid != "":
            print  "killing %s on %s" % (pid, hostname)
            cmd = "ssh %s@%s kill %s" % (username, hostname, pid);
            subprocess.call(cmd, shell=True);

    # Workers
    for (hostname, port) in workers:
        cmd = "ssh %s@%s $'ps aux | grep edu.washington.escience.myriad.parallel.Worker | grep %s | grep -v grep | awk \\'{print $2}\\''" % (username, hostname, username);
        pids = subprocess.check_output(cmd, shell=True).split('\n');
        for pid in pids:
            if pid != "":
                print  "killing %s on %s" % (pid, hostname)
                cmd = "ssh %s@%s kill %s" % (username, hostname, pid);
                subprocess.call(cmd, shell=True);

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

    # Extract the parameters
    try:
        USER = config.get('deployment', 'username')
    except:
        USER = getpass.getuser()
    MASTER = config.items('master')[0]
    WORKERS = config.items('workers')

    stop_all(MASTER, WORKERS, USER)

if __name__ == "__main__":
    main(sys.argv)
