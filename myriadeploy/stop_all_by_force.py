#!/usr/bin/env python

import ConfigParser
import subprocess
import sys
import getpass

def stop_all(master, workers, username):
    # Stop the Master
    (hostname, _) = master
    cmd = "ssh %s@%s $'ps aux | grep edu.washington.escience.myriad.daemon.MasterDaemon | grep %s | grep -v grep | awk \\'{print $2}\\''" % (username, hostname, username)
    pids = subprocess.check_output(cmd, shell=True).split('\n')
    for pid in pids:
        if pid != "":
            print  "killing %s on %s" % (pid, hostname)
            cmd = "ssh %s@%s kill %s" % (username, hostname, pid)
            subprocess.call(cmd, shell=True)

    # Workers
    done = set()
    for (hostname, _) in workers:
        if hostname in done:
            continue
        done.add(hostname)
        cmd = "ssh %s@%s $'ps aux | grep edu.washington.escience.myriad.parallel.Worker | grep %s | grep -v grep | awk \\'{print $2}\\''" % (username, hostname, username)
        pids = subprocess.check_output(cmd, shell=True).split('\n')
        for pid in pids:
            if pid != "":
                print  "killing %s on %s" % (pid, hostname)
                cmd = "ssh %s@%s kill %s" % (username, hostname, pid)
                subprocess.call(cmd, shell=True)

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
    except ConfigParser.NoOptionError:
        USER = getpass.getuser()
    def hostPortKeyToTuple(x):
        return tuple(x[0].split(','))
    MASTER = hostPortKeyToTuple(config.items('master')[0])
    WORKERS = [hostPortKeyToTuple(w) for w in config.items('workers')]

    stop_all(MASTER, WORKERS, USER)

if __name__ == "__main__":
    main(sys.argv)
