#!/usr/bin/env python

import subprocess
import sys
import logging

def make_deployment(config_file, fresh):
    "Copy the distribution (jar and libs and conf) to compute nodes."
    args = ["./using_deployment_utils.sh", config_file, "-deploy"]
    if fresh:
        args.append("-fresh_catalog")
    if subprocess.call(args):
        logging.error("Error copying distribution")


def main(argv):
    # Usage
    if len(argv) < 2 or len(argv) > 3 or (len(argv) == 3 and argv[2] != "-fresh"):
        print >> sys.stderr, "Usage: %s <deployment.cfg> <optional: -fresh>" % (argv[0])
        print >> sys.stderr, \
            "       deployment.cfg: \
            a configuration file modeled after deployment.cfg.sample"
        print >> sys.stderr, \
            "       -fresh: \
            if deploy with an empty master catalog"
        sys.exit(1)

    make_deployment(argv[1], len(argv) == 3)

if __name__ == "__main__":
    main(sys.argv)
