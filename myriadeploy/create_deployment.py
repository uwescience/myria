#!/usr/bin/env python

""" Create a Myria deployment file """

import sys
import argparse
from itertools import groupby


def get_deployment(path, coordinator_hostname, worker_hostnames, persist_uri,
                   name='myria', rest_port=8753, database_type='postgresql',
                   database_port=5432, heap_mem_fraction=None, driver_mem=None,
                   master_mem=None, worker_mem=None, master_cores=None,
                   worker_cores=None, debug=False, elastic_mode=False,
                   database_username=None, database_password=None,
                   coordinator_port=9001, worker_ports=None,
                   worker_base_port=8001, worker_directories=None,
                   worker_databases=None):
    """ Generates a Myria deployment file with the given configuration """
    return (_get_header(path, name, rest_port, database_type, database_port,
                        database_username, database_password, debug,
                        elastic_mode) +
            _get_coordinator(coordinator_hostname, coordinator_port) +
            _get_runtime(heap_mem_fraction, driver_mem, master_mem,
                         worker_mem, master_cores, worker_cores) +
            _get_workers(worker_hostnames, worker_ports, worker_base_port,
                         worker_directories, worker_databases) +
            _get_persist(persist_uri)
            )


def _get_header(path, name='myria', rest_port=8753, database_type='postgresql',
                database_port=5432, database_username=None,
                database_password=None, debug=False, elastic_mode=False):
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
    if debug:
        header += 'debug = True\n'
    if elastic_mode:
        header += 'elastic_mode = True\n'

    return header + '\n'


def _get_coordinator(hostname, port=9001):
    """ Generates the coordinator section of a Myria deployment file """
    return '[master]\n' \
           '0 = {}:{}\n\n'.format(hostname, port)


def _get_runtime(heap_mem_fraction=None, driver_mem=None, master_mem=None,
                 worker_mem=None, master_cores=None, worker_cores=None):
    """ Generates the runtime section of a Myria deployment file """
    runtime = '[runtime]\n'
    if driver_mem:
        runtime += 'container.driver.memory.size.gb = %s\n' % driver_mem
    if master_mem:
        runtime += 'container.master.memory.size.gb = %s\n' % master_mem
        runtime += 'jvm.master.heap.size.min.gb = %s\n' % (
            master_mem * heap_mem_fraction)
        runtime += 'jvm.master.heap.size.max.gb = %s\n' % (
            master_mem * heap_mem_fraction)
    if worker_mem:
        runtime += 'container.worker.memory.size.gb = %s\n' % worker_mem
        runtime += 'jvm.worker.heap.size.min.gb = %s\n' % (
            worker_mem * heap_mem_fraction)
        runtime += 'jvm.worker.heap.size.max.gb = %s\n' % (
            worker_mem * heap_mem_fraction)
    if master_cores:
        runtime += 'container.master.vcores.number = %s\n' % master_cores
    if worker_cores:
        runtime += 'container.worker.vcores.number = %s\n' % worker_cores
    if not master_mem and not worker_mem \
            and not master_cores and not worker_cores:
        runtime += '# No runtime options specified\n'
    return runtime + '\n'


def _get_workers(hostnames, ports=None, base_port=8001,
                 directories=None, database_names=None):
    """ Generates the worker section of a Myria deployment file """
    workers = '[workers]\n'

    if not ports:
        # FIXME: we can't sort by hostname because cloud deployment scripts
        # need to sort by a stable, monotonically increasing ID for elasticity.
        # We should add an assert that verifies all duplicate hostnames are
        # contiguous, even if the list is unsorted by hostname.
        # hostnames = sorted(hostnames)
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


def _get_persist(uri):
    """ Generates the persistence section of a Myria deployment file """
    return '[persist]\n' \
           'persist_uri = {}\n\n'.format(uri)


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
        '--master-cores-number', type=int, dest='master_cores',
        help='CPUs allocated to the master (e.g., "2")')
    parser.add_argument(
        '--worker-cores-number', type=int, dest='worker_cores',
        help='CPUs allocated to each worker (e.g., "2")')
    parser.add_argument(
        '--driver-memory-size-gb', type=float, dest='driver_mem',
        help='Memory in GB allocated to the driver (e.g., "0.25")')
    parser.add_argument(
        '--master-memory-size-gb', type=float, dest='master_mem',
        help='Memory in GB allocated to the master (e.g., "2.0")')
    parser.add_argument(
        '--worker-memory-size-gb', type=float, dest='worker_mem',
        help='Memory in GB allocated to each worker (e.g., "2.0")')
    parser.add_argument(
        '--heap-memory-fraction', type=float, dest='heap_mem_fraction',
        help='Fraction of container memory used by JVM heap (e.g., "0.9")')
    parser.add_argument(
        '--persist-uri', dest='persist_uri', type=str,
        help='URI of persistence endpoint')
    parser.add_argument(
        '--debug', default=False, action='store_true',
        help='Enable debugging support')
    parser.add_argument(
        '--elastic-mode', dest='elastic_mode', default=False,
        action='store_true', help='Enable elastic mode')

    print get_deployment(**vars(parser.parse_args(argv[1:])))

if __name__ == "__main__":
    main(sys.argv)
