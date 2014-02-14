#!/usr/bin/env python

import re
import sys
import argparse


#parse args
parser = argparse.ArgumentParser(
    description='collect running time of workers of a query')
parser.add_argument("log", type=str, help="log file path")
parser.add_argument("worker_number", type=int, help="number of workers")
parser.add_argument("query", type=int, help="query id")
args = parser.parse_args()


def collect_worker_time(path, query_id, worker_id):

    regex_operator = re.compile(r'INFO  \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d* .Nonblocking query executor#\d*. WorkerQueryPartition\$\d* - Query #(\d*) executed for ([\w\W]*)')

    lines = [line.strip() for line in open(path)]
    lines = [line for line in lines if regex_operator.match(line)]
    tuples = [re.findall(r'INFO  \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d* .Nonblocking query executor#\d*. WorkerQueryPartition\$\d* - Query #(\d*) executed for (\d*h )?(\d*m )?(\d*\.\d+|\d+)s', line)
              for line in lines]
    tuples = [i[0] for i in tuples if i[0][0] == str(query_id)]
    if len(tuples) == 0:
        print >> sys.stderr, "can not find running time of query %d" % query_id
        sys.exit(1)
    elif len(tuples) > 1:
        print >> sys.stderr, "find running time of query %d\
         more than once" % query_id
        sys.exit(1)
    time = 0.0
    if tuples[0][1]:
        time = time + int(tuples[0][1][:-2]) * 3600
    if tuples[0][2]:
        time = time + int(tuples[0][2][:-2]) * 60
    if tuples[0][3]:
        time = time + float(tuples[0][3])
    print "%d, %f" % (worker_id, time)


def main():
    print "worker_id,running_time"
    number_of_workers = args.worker_number
    for i in range(1, number_of_workers + 1):
        collect_worker_time(args.log + "/worker_%d_stdout" % i, args.query, i)

if __name__ == "__main__":
    main()
