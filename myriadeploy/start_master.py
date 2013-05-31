#!/usr/bin/env python

"Start the Myria master in the specified deployment."

import myriadeploy
import setup_cluster

import subprocess
import sys

def start_master(config):
    "Start the Myria master in the specified deployment."
    description = config['description']
    default_path = config['path']
    master = config['master']
    username = config['username']
    max_heap_size = config['max_heap_size']
    rest_port = config['rest_port']

    (hostname, _, path) = setup_cluster.get_host_port_path(master, default_path)
    cmd = "cd %s/%s-files; nohup java -cp myriad-0.1.jar:conf -Djava.library.path=sqlite4java-282 " % (path, description) + max_heap_size + " edu.washington.escience.myriad.daemon.MasterDaemon %s %s 0</dev/null 1>master_stdout 2>master_stderr &" % (description,rest_port)
    args = ["ssh", "%s@%s" % (username, hostname), cmd]
    if subprocess.call(args):
        print >> sys.stderr, "error starting master %s" % (hostname)
    print hostname

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
