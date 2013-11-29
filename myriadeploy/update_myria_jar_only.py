#!/usr/bin/env python

import subprocess
import sys


def copy_distribution(config_file):
    "Copy the distribution (jar and libs and conf) to compute nodes."
    args = ["./using_deployment_utils.sh", config_file, "-copy_distribution"]
    if subprocess.call(args):
        raise Exception("Error copying distribution")


def main(argv):
    # Usage
    if len(argv) != 2:
        print >> sys.stderr, "Usage: %s <deployment.cfg>" % (argv[0])
        print >> sys.stderr, \
            "       deployment.cfg: \
            a configuration file modeled after deployment.cfg.sample"
        sys.exit(1)

    # Step 1: Copy over libs, "conf", myria
    copy_distribution(argv[1])

if __name__ == "__main__":
    main(sys.argv)
