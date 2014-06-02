#!/usr/bin/env python

"Start all Myria workers in the specified deployment."

import subprocess
import sys

def start_workers(config_file):
    args = ["./using_deployment_utils.sh", config_file, "-start_workers"]
    if subprocess.call(args):
        raise Exception("Error starting workers")

def main(argv):
    "Start all Myria workers in the specified deployment."
    # Usage
    if len(argv) != 2:
        print >> sys.stderr, "Usage: %s <deployment.cfg>" % (argv[0])
        print >> sys.stderr, "       deployment.cfg: a configuration file modeled after deployment.cfg.sample"
        sys.exit(1)

    # Start the workers
    start_workers(argv[1])

if __name__ == "__main__":
    main(sys.argv)
