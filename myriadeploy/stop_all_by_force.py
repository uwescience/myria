#!/usr/bin/env python

"Kill all Myria processes on the nodes in this cluster."

import myriadeploy

import subprocess
import sys

def get_hostname(node):
    if len(node) == 2:
        (hostname, _) = node
    else:
        (hostname, _, _) = node
    return hostname


def stop_all(config):
    "Kill all Myria processes on the nodes in this cluster."
    master = config['master']
    workers = config['workers']
    username = config['username']

    # Stop the Master
    hostname = get_hostname(master)
    cmd = "ssh -o ConnectTimeout=6 %s@%s $'ps aux | grep edu.washington.escience.myria.daemon.MasterDaemon | grep %s | grep -v grep | awk \\'{print $2}\\''" % (username, hostname, username)
    pids = subprocess.check_output(cmd, shell=True).split('\n')
    for pid in pids:
        if pid != "":
            print  "killing %s on %s" % (pid, hostname)
            cmd = "ssh -o ConnectTimeout=6 %s@%s kill -9 %s" % (username, hostname, pid)
            subprocess.call(cmd, shell=True)

    # Workers
    done = set()
    for worker in workers:
        hostname = get_hostname(worker)
        if hostname in done:
            continue
        done.add(hostname)
        cmd = "ssh -o ConnectTimeout=6 %s@%s $'ps aux | grep edu.washington.escience.myria.parallel.Worker | grep %s | grep -v grep | awk \\'{print $2}\\''" % (username, hostname, username)
        try:
            pids = subprocess.check_output(cmd, shell=True).split('\n')
        except:
            continue
        for pid in pids:
            if pid != "":
                print  "killing %s on %s" % (pid, hostname)
                cmd = "ssh -o ConnectTimeout=6 %s@%s kill -9 %s" % (username, hostname, pid)
                subprocess.call(cmd, shell=True)

def main(argv):
    "Kill all Myria processes on the nodes in this cluster."
    # Usage
    if len(argv) != 2:
        print >> sys.stderr, "Usage: %s <deployment.cfg>" % (argv[0])
        print >> sys.stderr, "       deployment.cfg: a configuration file modeled after deployment.cfg.sample"
        sys.exit(1)

    config = myriadeploy.read_config_file(argv[1])

    stop_all(config)

if __name__ == "__main__":
    main(sys.argv)
