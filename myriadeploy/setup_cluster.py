#!/usr/bin/env python

import subprocess
import sys
import argparse

def make_deployment(input_args):
    "Copy the distribution (jar and libs and conf) to compute nodes."
    args = ["./using_deployment_utils.sh", input_args.config_file, "--deploy"]
    if input_args.clean_catalog:
        args.append("--clean_catalog")
    if subprocess.call(args):
        logging.error("Error copying distribution")


def main(argv):
    parser = argparse.ArgumentParser(description='Setup a Myria cluster')
    parser.add_argument('config_file',
        help='The deployment config file')
    parser.add_argument('--clean_catalog', action='store_true',
        help='If deploying with a new master catalog')

    make_deployment(parser.parse_args())

if __name__ == "__main__":
    main(sys.argv)
