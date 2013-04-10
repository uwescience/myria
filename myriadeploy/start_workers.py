#!/usr/bin/env python

import ConfigParser
import subprocess
import sys
import getpass

def start_workers(description, root, workers, username, MAX_MEM):
    worker_id = 0
    for (hostname, _) in workers:
        worker_id = worker_id + 1
        cmd = "cd %s/%s-files; nohup java -cp myriad-0.1.jar:conf -Djava.library.path=sqlite4java-282 " % (root, description) + MAX_MEM + " edu.washington.escience.myriad.parallel.Worker --workingDir %s/worker_%d 0</dev/null 1>worker_%d_stdout 2>worker_%d_stderr &" % (description, worker_id, worker_id, worker_id)
        args = ["ssh", "%s@%s" % (username, hostname), cmd]
        if subprocess.call(args):
            print >> sys.stderr, "error starting worker %s" % (hostname)
        print hostname

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
    DESCRIPTION = config.get('deployment', 'name')
    EXPT_ROOT = config.get('deployment', 'path')
    try:
        USER = config.get('deployment', 'username')
    except ConfigParser.NoOptionError:
        USER = getpass.getuser()
    def hostPortKeyToTuple(x):
        return tuple(x[0].split(','))
    WORKERS = [hostPortKeyToTuple(w) for w in config.items('workers')]
    try:
        MAX_MEM = config.get('deployment', 'max_heap_size')
    except ConfigParser.NoOptionError:
        MAX_MEM = ""

    # Start the workers
    start_workers(DESCRIPTION, EXPT_ROOT, WORKERS, USER, MAX_MEM)

if __name__ == "__main__":
    main(sys.argv)
