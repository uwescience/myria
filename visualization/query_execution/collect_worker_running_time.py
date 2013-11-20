#!/usr/bin/env python

import re
import subprocess
import sys

def collect_worker_time(path, query_id, worker_id):           

    regex_operator = re.compile(r'INFO  \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d* .Nonblocking query executor#\d*. WorkerQueryPartition\$\d* - Query #(\d*) executed for (\d*)h (\d*)m (\d*\.\d+|\d+)s')

    lines = [ line.strip() for line in open(path)]
    lines = [ line for line in lines if regex_operator.match(line) ]
    tuples = [ re.findall(r'INFO  \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d* .Nonblocking query executor#\d*. WorkerQueryPartition\$\d* - Query #(\d*) executed for (\d*)h (\d*)m (\d*\.\d+|\d+)s', line) 
                for line in lines] 
    tuples = [i[0] for i in tuples if i[0][0] == str(query_id)]
    if len(tuples) == 0: 
        print >> sys.stderr, "can not find running time of query %d"%query_id
        sys.exit(1)
    elif len(tuples) > 1:
        print >> sys.stderr, "find running time of query %d more than once"%query_id
        sys.exit(1)
    print "%d,%f"%(worker_id, int(tuples[0][1])*3600+int(tuples[0][2])*60+float(tuples[0][3]))

def main(argv):
# Usage
    if len(argv) != 4:
        print >> sys.stderr, "Usage: %s <log_file_path> <# of workers> <query id>" % (argv[0])
        print >> sys.stderr, "       log_file_path "
        print >> sys.stderr, "       # of workers "
        print >> sys.stderr, "       query id "
        sys.exit(1)

    print "worker_id,running_time"
    number_of_workers = int(argv[2])
    for i in range(1,number_of_workers+1):
        collect_worker_time(argv[1]+"/worker_%d_stdout"%i,int(argv[3]),i)

if __name__ == "__main__":
    main(sys.argv)
