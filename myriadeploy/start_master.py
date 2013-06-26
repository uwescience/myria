#!/usr/bin/env python

"Start the Myria master in the specified deployment."

import myriadeploy
import setup_cluster

import subprocess
import sys
import time
import urllib2

def start_master(config):
    "Start the Myria master in the specified deployment."
    description = config['description']
    default_path = config['path']
    master = config['master']
    username = config['username']
    max_heap_size = config['max_heap_size']
    rest_port = config['rest_port']

    (hostname, _, path) = setup_cluster.get_host_port_path(master, default_path)
    cmd = "cd %s/%s-files; nohup java -cp 'libs/*' -Dlog4j.configuration=log4j.properties -Djava.util.logging.config.file=conf/logging.properties -Djava.library.path=sqlite4java-282 " % (path, description) + max_heap_size + " edu.washington.escience.myriad.daemon.MasterDaemon %s %s 0</dev/null 1>master_stdout 2>master_stderr &" % (description,rest_port)
    args = ["ssh", "%s@%s" % (username, hostname), cmd]
    if subprocess.call(args):
        print >> sys.stderr, "error starting master %s" % (hostname)
        sys.exit(1)

    # Wait for master to start
    master_alive_url = "http://%s:%s/workers/alive" % (hostname, rest_port)
    start = time.time()
    while True:
        try:
            resp = urllib2.urlopen(master_alive_url)
            if resp.getcode() == 200:
                break
        except:
            pass
        time.sleep(0.01)
        if time.time() - start > 20:
            print >> sys.stderr, "after 20s master %s is not alive" % hostname
            sys.exit(1)
    print "master started at %s" % hostname

def main(argv):
    "Start the Myria master in the specified deployment."
    # Usage
    if len(argv) != 2:
        print >> sys.stderr, "Usage: %s <deployment.cfg>" % (argv[0])
        print >> sys.stderr, "       deployment.cfg: a configuration file modeled after deployment.cfg.sample"
        sys.exit(1)

    # Read config
    config = myriadeploy.read_config_file(argv[1])

    # Start master
    start_master(config)

if __name__ == "__main__":
    main(sys.argv)
