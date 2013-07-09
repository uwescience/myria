#!/usr/bin/env python

import myriadeploy

import subprocess
import sys

def make_catalog(config_file):
    """Creates a Myria catalog (running the Java program to do so) from the
given deployment configuration."""
    args = ["./run_catalog_maker.sh", config_file]
    if subprocess.call(args):
        raise Exception("Error making the catalogs");

def copy_catalogs(config_file):
    """Copies the master and worker catalogs to the remote hosts."""
    args = ["./using_deployment_utils.sh", config_file, "-copy_master_catalog"]
    if subprocess.call(args):
        raise Exception("Error copying master catalog");
    args = ["./using_deployment_utils.sh", config_file, "-copy_worker_catalogs"]
    if subprocess.call(args):
        raise Exception("Error copying worker catalogs");

def copy_distribution(config_file):
    "Copy the distribution (jar and libs and conf) to compute nodes."
    args = ["./using_deployment_utils.sh", config_file, "-copy_distribution"]
    if subprocess.call(args):
        raise Exception("Error copying distribution to %s" % (hostname,))

def main(argv):
    # Usage
    if len(argv) != 2:
        print >> sys.stderr, "Usage: %s <deployment.cfg>" % (argv[0])
        print >> sys.stderr, "       deployment.cfg: a configuration file modeled after deployment.cfg.sample"
        sys.exit(1)

    # Step 1: make the Catalog
    make_catalog(argv[1])

    # Step 2: Copy each catalog over
    copy_catalogs(argv[1])

    # Step 3: Copy over java, libs, myriad, conf
    copy_distribution(argv[1])

if __name__ == "__main__":
    main(sys.argv)
