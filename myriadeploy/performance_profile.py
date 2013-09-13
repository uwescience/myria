#!/usr/bin/env python

import re
import get_logs
import subprocess
import sys
import myriadeploy
import time
import json

def read_json(filename):
    with open(filename, 'r') as f:
        return json.load(f)

def pretty_json(obj):
    return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ':'))

def getQueryTaskTree(query_file):

def generateProfile(config,query_id):

    """Copies the master and worker catalogs to the remote hosts."""
    description = config['description']
    default_path = config['path']
    master = config['master']
    workers = config['workers']
    username = config['username']

    regex_operator = re.compile(r'^DEBUG \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d* .Nonblocking query executor#\d*. (Operator) - .query_id#\d*.')
    
    # create gantt tasks
    tasks = []
    taskNames = []
    for (i, worker) in enumerate(workers):
        
        # Workers are numbered from 1, not 0
        worker_id = i + 1
        lines = [ line.strip() for line in open("./%s/worker_%s_stdout"%(description,worker_id))]
        lines = [ line for line in lines if regex_operator.match(line) ]
        
        # parse infomation from each log message
        tuples = [ re.findall(r'DEBUG (\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2}),(\d*) .Nonblocking query executor#\d*. Operator - .query_id#(\d*).edu\.washington\.escience\.myria\.\w*\.(\w*)@(\w*):([\w|\W]*)', line) for line in lines]
        tuples = [i[0] for i in tuples]
        tuples = [ (i[8], 
                    {
                        'year':int(i[0]),
                        'month':int(i[1]),
                        'day':int(i[2]),
                        'hours':int(i[3]),
                        'minutes':int(i[4]),
                        'seconds':int(i[5]),
                        'milliseconds':int(i[6]),
                        'query_id':i[7],
                        'operator':i[8], 
                        'hashcode':i[9], 
                        'message':i[10]
                    }
                    )
                   for i in tuples ]
        

        # filter out unrelevant queries
        tuples = [ i for i in tuples if int(i[1]['query_id']) == query_id ]

        # group by operators
        operators = {}
        for k,v in tuples:
            v_hash = v['hashcode']
            del v['hashcode']
            if operators.has_key(k):
                if operators[k].has_key(v_hash):
                    operators[k][v_hash].append(v)
                else:
                    operators[k][v_hash] = [v]
            else:
                operators[k]={v_hash:[v]};
        
        # get tasks in each worker
        for k,v in operators.items():
            for hashcode,entries in v.items():
                task = {}
                if len(entries) !=2:
                    print >> sys.stderr, " Operator %s (%s) appears %d times! "%(k,hashcode,len(entry))
                    return
                for e in entries:
                    if e['message']=='begin to process':
                        task['begin_date'] = {'year': e['year'], 'month':e['month'], 'day':e['day'], 'hours':e['hours'], 'minutes':e['minutes'], 'seconds':e['seconds'], 'milliseconds':e['milliseconds']}    
                    else:    
                        task['end_date'] = {'year': e['year'], 'month':e['month'], 'day':e['day'], 'hours':e['hours'], 'minutes':e['minutes'], 'seconds':e['seconds'], 'milliseconds':e['milliseconds']}  
                #taskName = "worker_%d.%s"%(worker_id,k)
                taskName = k
                task['taskName'] = taskName
                tasks.append(task)
                if taskName not in taskNames:
                    taskNames.append(taskName)
    

    taskNames.sort()
    output = {
        'tasks':tasks,
        'taskNames':taskNames
    }
    print pretty_json(output)

def main(argv):
# Usage
    if len(argv) != 3:
        print >> sys.stderr, "Usage: %s <deployment.cfg> <json_query_plan>" % (argv[0])
        print >> sys.stderr, "       deployment.cfg: a configuration file modeled after deployment.cfg.sample"
        print >> sys.stderr, "       json_query_plan: target json query plan"
        sys.exit(1)

    config = myriadeploy.read_config_file(argv[1])
    get_logs.getlog(config)
    generateProfile(config,2)

if __name__ == "__main__":
    main(sys.argv)

