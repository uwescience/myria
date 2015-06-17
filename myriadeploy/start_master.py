#!/usr/bin/env python

"Start the Myria master in the specified deployment."

import subprocess
import sys
import logging


def start_master(config_file):
    args = ["./using_deployment_utils.sh", config_file, "--start_master"]
    if subprocess.call(args):
        sys.exit(1)


def main(argv):
    "Start the Myria master in the specified deployment."
    # Usage
    if len(argv) != 2:
        print >> sys.stderr, "Usage: %s <deployment.cfg>" % (argv[0])
        print >> sys.stderr, "       deployment.cfg: a configuration file modeled after deployment.cfg.sample"
        sys.exit(1)

    # Start master
    start_master(argv[1])

if __name__ == "__main__":
    main(sys.argv)
