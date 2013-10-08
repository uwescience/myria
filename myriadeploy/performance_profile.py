#!/usr/bin/env python

import re
import subprocess
import sys
import time
import json
import datetime

def read_json(filename):
    with open(filename, 'r') as f:
        return json.load(f)

def pretty_json(obj):
    return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ':'))

def serialize_datetime(datetime):
    result = {
        'year': datetime.year,
        'month': datetime.month,
        'day': datetime.day,
        'hour': datetime.hour,
        'minute': datetime.minute,
        'second': datetime.second,
        'millisecond': datetime.microsecond/1000,
    }
    return result

def generateProfile(path,query_id):

    # filter regex   
    regex_operator = re.compile(r'INFO  \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d* .Nonblocking query executor#\d*. Operator - .query_id#\d*.')
    
    # create gantt tasks
    tasks = []
    taskNames = []
            
    # Workers are numbered from 1, not 0
    lines = [ line.strip() for line in open(path)]
    lines = [ line for line in lines if regex_operator.match(line) ]
    
    # parse infomation from each log message
    tuples = [ re.findall(r'INFO  (\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2}),(\d*) .Nonblocking query executor#\d*. Operator - .query_id#(\d*)..([\w(),]*)@(\w*)..edu\.washington\.escience\.myria\.\w*\.(\w*)@(\w*).:([\w|\W]*)', line) 
                for line in lines]
    tuples = [i[0] for i in tuples]
    tuples = [(i[8],
                {
                    'time': datetime.datetime(int(i[0]),int(i[1]),int(i[2]),int(i[3]), int(i[4]), int(i[5]), int(i[6])*1000),
                    'query_id':i[7],
                    'operator_name':i[8],
                    'fragment_id': i[9], 
                    'operator_type': i[10],
                    'hashcode':i[11], 
                    'message':i[12]
                })
            for i in tuples ]
    
    # filter out unrelevant queries
    tuples = [ i for i in tuples if int(i[1]['query_id']) == query_id ]

    # retrieve execution time information
    for tp in tuples:
        match = re.search(r' executionTime (\d+) ms', tp[1]['message'])
        if match:
            tp[1]['executionTime'] = match.group(1)
            tp[1]['message'] = 'executionTime'
    

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
            if len(entries) !=3:
                print >> sys.stderr, " Operator %s (%s) appears %d times! "%(k,hashcode,len(entries))
                return
            for e in entries:
                if e['message']=='begin to process':
                    #task['begin_date'] = serialize_datetime(e['time'])
                    task['begin_date'] = e['time']
                elif e['message']=='End of Processing (EOS)':    
                    #task['end_date'] = serialize_datetime(e['time'])
                    task['end_date'] = e['time']
                elif e['message']=='executionTime':
                    task['executionTime'] = int(e['executionTime'])

            taskName = k
            task['taskName'] = taskName
            tasks.append(task)
            if taskName not in taskNames:
                taskNames.append(taskName)

    #generate the fake execution start time
    for task in tasks:
        task['execution_start_time'] = task['end_date'] - datetime.timedelta(milliseconds=task['executionTime'])
        if task['execution_start_time']<task['begin_date'] :
            task['execution_start_time'] = task['begin_date']
            task['executionTime'] = int(round((task['end_date']-task['begin_date']).total_seconds()*1000))

        # serialize the date    
        task['execution_start_time'] = serialize_datetime(task['execution_start_time'])
        task['begin_date'] = serialize_datetime(task['begin_date'])
        task['end_date'] = serialize_datetime(task['end_date'])


    taskNames.sort()
    output = {
        'tasks':tasks,
        'taskNames':taskNames
    }
    print pretty_json(output)

def main(argv):
# Usage
    if len(argv) != 2:
        print >> sys.stderr, "Usage: %s <deployment.cfg> <json_query_plan>" % (argv[0])
        print >> sys.stderr, "       deployment.cfg: a configuration file modeled after deployment.cfg.sample"
        print >> sys.stderr, "       json_query_plan: target json query plan"
        sys.exit(1)

    #config = myriadeploy.read_config_file(argv[1])
    #get_logs.getlog(config)
    generateProfile(argv[1],28)

if __name__ == "__main__":
    main(sys.argv)

