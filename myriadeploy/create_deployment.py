#!/usr/bin/env python

""" Create a Myria deployment file """

import sys
import argparse
from itertools import groupby


def get_deployment(path, coordinator_hostname, worker_hostnames, name='myria',
                   rest_port=8753, database_type='postgresql',
                   database_port=5432, heap=None, debug=False,
                   database_username=None, database_password=None,
                   coordinator_port=9001, worker_ports=None,
                   worker_base_port=8001, worker_directories=None,
                   worker_databases=None):
    """ Generates a Myria deployment file with the given configuration """
    return (_get_header(path, name, rest_port, database_type, database_port,
                        heap, debug, database_username, database_password) +
            _get_coordinator(coordinator_hostname, coordinator_port) +
            _get_workers(worker_hostnames, worker_ports, worker_base_port,
                         worker_directories, worker_databases))


def _get_header(path, name='myria', rest_port=8753, database_type='postgresql',
                database_port=5432, heap=None, debug=False,
                database_username=None, database_password=None):
    """ Generates the header section of a Myria deployment file """
    header = ('[deployment]\n'
              'name = {name}\n'
              'path = {path}\n'
              'dbms = {dbms}\n'
              'database_name = {name}\n'
              'database_port = {database_port}\n'
              'rest_port = {rest_port}\n'.format(
                  name=name, path=path, dbms=database_type,
                  database_port=database_port, rest_port=rest_port))
    if database_username:
        header += 'username = %s\n' % database_username
    if database_password:
        header += 'database_password = %s\n' % database_password
    if heap:
        header += 'max_heap_size = -Xmx%s\n' % heap
    if debug:
        header += 'debug_mode = True\n'

    return header + '\n'


def _get_coordinator(hostname, port=9001):
    """ Generates the coordinator section of a Myria deployment file """
    return '[master]\n' \
           '0 = {}:{}\n\n'.format(hostname, port)


def _get_workers(hostnames, ports=None, base_port=8001,
                 directories=None, database_names=None):
    """ Generates the worker section of a Myria deployment file """
    workers = '[workers]\n'

    if not ports:
        hostnames = sorted(hostnames)
        ports = [offset + base_port
                 for hostname, group in groupby(hostnames)
                 for offset in xrange(len(list(group)))]

    # Make sure these lists are at least as long as hostnames
    directories = (directories or []) + [''] * len(hostnames)
    database_names = (database_names or []) + [''] * len(hostnames)

    for index, (hostname, port, directory, database_name) in enumerate(zip(
            hostnames, ports, directories, database_names)):
        workers += '{} = {}:{}:{}:{}\n'.format(index + 1, hostname, port,
                                               directory, database_name)

    return workers


def main(argv):
    """ Argument parsing wrapper for generating a Myria deployment file """
    parser = argparse.ArgumentParser(
        description='Create a Myria deployment file')
    parser.add_argument(
        'path', type=str,
        help='Installation path for catalog and worker metadata storage')
    parser.add_argument(
        'coordinator_hostname', metavar='coordinator', type=str,
        help='Hostname for the coordinator')
    parser.add_argument(
        'worker_hostnames', metavar='workers', type=str, nargs='+',
        help='One or more worker hostnames')

    parser.add_argument(
        '--name', type=str, default='myria',
        help='Name identifying this installation')
    parser.add_argument(
        '--rest-port', dest='rest_port', type=int, default=8753,
        help='Port for REST requests')
    parser.add_argument(
        '--database-type', dest='database_type', type=str,
        default='postgresql', help='Database type for Myria storage layer')
    parser.add_argument(
        '--database-port', dest='database_port', type=int, default='5432',
        help='Port used to connect to storage layer')
    parser.add_argument(
        '--database-username', dest='database_username', type=str,
        default=None, help='Username for connecting to storage layer')
    parser.add_argument(
        '--database-password', dest='database_password', type=str,
        default=None, help='Password for connecting to storage layer')
    parser.add_argument(
        '--coordinator-port', dest='coordinator_port', type=int,
        default=8001, help='Port for coordinator communication')

    parser.add_argument(
        '--worker-ports', dest='worker_ports', type=int, nargs='*',
        default=None, help='One or more ports for worker communication')
    parser.add_argument(
        '--worker-base-port', dest='worker_base_port', type=int,
        default=9001, help='Base port for worker communication '
                           '(when worker-ports not specified)')
    parser.add_argument(
        '--worker-directories', dest='worker_directories', type=str, nargs='*',
        default=None, help='One or more worker directories '
                           '(default is [path])')
    parser.add_argument(
        '--worker-databases', dest='worker_databases', type=str, nargs='*',
        default=None, help='One or more worker database names '
                           '(default is [--name])')

    parser.add_argument(
        '--heap', type=str, help='Java VM heap size (e.g., "-Xmx2g") '
                                 'and/or other parameters')
    parser.add_argument(
        '--debug', default=False, action='store_true',
        help='Enable debugging support')

    print get_deployment(**vars(parser.parse_args(argv[1:])))

if __name__ == "__main__":
    main(sys.argv)
