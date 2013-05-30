#!/usr/bin/env python

"Start all Myria workers in the specified deployment."

import myriadeploy

import subprocess
import sys

def start_workers(config):
    "Start all Myria workers in the specified deployment."
    description = config['description']
    workers = config['workers']
    username = config['username']
    max_heap_size = config['max_heap_size']

    worker_id = 0
    for (hostname, _, path) in workers:
        worker_id = worker_id + 1
        cmd = "cd %s/%s-files; nohup java -cp myriad-0.1.jar:conf -Djava.library.path=sqlite4java-282 " % (path, description) + max_heap_size + " edu.washington.escience.myriad.parallel.Worker --workingDir %s/worker_%d 0</dev/null 1>worker_%d_stdout 2>worker_%d_stderr &" % (description, worker_id, worker_id, worker_id)
        args = ["ssh", "%s@%s" % (username, hostname), cmd]
        if subprocess.call(args):
            print >> sys.stderr, "error starting worker %s" % (hostname)
        print hostname

def main(argv):
    "Start all Myria workers in the specified deployment."
    # Usage
    if len(argv) != 2:
        print >> sys.stderr, "Usage: %s <deployment.cfg>" % (argv[0])
        print >> sys.stderr, "       deployment.cfg: a configuration file modeled after deployment.cfg.sample"
        sys.exit(1)

    # Parse the configuration
    config = myriadeploy.read_config_file(argv[1])

    # Start the workers
    start_workers(config)

if __name__ == "__main__":
    main(sys.argv)
