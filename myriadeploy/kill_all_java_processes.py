#!/usr/bin/env python

"Kill all Java processes owned by the current user on the given cluster."

import myriadeploy

import subprocess
import sys
import logging


def get_hostname(node):
    return node[0]


def kill_command(host_entry, command):
    host = get_hostname(host_entry)
    cmd = ['ssh', host, 'killall -KILL -v ' + command]
    subprocess.call(cmd)


def stop_all(config, command):
    master = config['master']
    workers = config['workers']

    # Stop the Master
    kill_command(master, command)

    for worker in workers:
        kill_command(worker, command)


def main(argv):
    if len(argv) < 2:
        print >> sys.stderr, "Usage: %s <deployment.cfg> <command>\n By default command is java." % (argv[0])
        sys.exit(1)

    command = 'java'
    config = myriadeploy.read_config_file(argv[1])

    if len(argv) > 2:
        command = argv[2]

    stop_all(config, command)

if __name__ == "__main__":
    main(sys.argv)
