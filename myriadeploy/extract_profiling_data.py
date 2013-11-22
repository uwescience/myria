#!/usr/bin/env python

from collections import defaultdict
import re
import subprocess
import sys
import time
import json
import datetime
import copy
import myriadeploy
import itertools

# root operators
root_operators = set(['LocalMultiwayProducer',
                      'CollectProducer',
                      'ShuffleProducer',
                      'BroadcastProducer',
                      'SinkRoot',
                      'DbInsert'])
 
# By default, all operators have no children
children = defaultdict(list)
# Populate the list for all operators that do have children
children['CollectProducer'] = ['arg_child']
children['EOSController'] = ['arg_child']
children['IDBInput'] = ['arg_initial_input', 'arg_iteration_input', 'arg_eos_controller_input']
children['RightHashJoin'] = ['arg_child1', 'arg_child2']
children['RightHashCountingJoin'] = ['arg_child1', 'arg_child2']
children['SymmetricHashJoin'] = ['arg_child1', 'arg_child2']
children['LocalMultiwayProducer'] = ['arg_child']
children['MultiGroupByAggregate'] = ['arg_child']
children['SingleGroupByAggregate'] = ['arg_child']
children['ShuffleProducer'] = ['arg_child']
children['DbInsert'] = ['arg_child']
children['Aggregate'] = ['arg_child']
children['Apply'] = ['arg_child']
children['Filter'] = ['arg_child']
children['UnionAll'] = ['arg_children']
children['Merge'] = ['arg_children']
children['ColumnSelect'] = ['arg_child']
children['SymmetricHashCountingJoin'] = ['arg_child1', 'arg_child2']
children['BroadcastProducer'] = ['arg_child']
children['HyperShuffleProducer'] = ['arg_child']
children['SinkRoot'] = ['arg_child']
children['DupElim'] = ['arg_child']
children['Rename'] = ['arg_child']   

# deserialize json
def read_json(filename):
    with open(filename, 'r') as f:
        return json.load(f)
# print json
def pretty_json(obj):
    return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ':'))

# get operator name type mapping
def name_type_mapping(query_plan_file):
    plan = read_json(query_plan_file)
    fragments = plan['fragments']
    mapping = dict()
    for fragment in fragments:
        for operator in fragment['operators']:
            if mapping.has_key(operator['op_name']):
                print >> sys.stderr, "       dup names "
                sys.exit(1)
            else:
                mapping[operator['op_name']]= operator['op_type']
    return mapping

# unify fragment
def get_parent(fragment):  
    ret = dict()
    for operator in fragment['operators']:
        for field in children[operator['op_type']]:
            if not isinstance(operator[field], list):
                ret[ operator[field] ] = operator['op_name'] 
            else:
                for child in operator[field]:
                    ret[operator[field]] = operator['op_name']      
    return ret

def generateProfile(path,query_id,fragment_id,query_plan_file):  
    # get operator name to type mapping
    mapping = name_type_mapping(query_plan_file);

    # create gantt tasks
    tasks = []
    taskNames = []
            
    # Workers are numbered from 1, not 0
    lines = [ line.strip() for line in open(path)]
    
    # parse infomation from each log message
    tuples = [ re.findall(r'.query_id#(\d*)..([\w(),]*)@(\w*)..(\d*).:([\w|\W]*)', line) 
                for line in lines]
    tuples = [i[0] for i in tuples]
    tuples = [(i[1],
                {
                    'time': long(i[3]),
                    'query_id':i[0],
                    'name':i[1],
                    'fragment_id': i[2], 
                    'message': i[4]
                })
            for i in tuples ]
    
    # filter out unrelevant queries
    tuples = [ i for i in tuples if int(i[1]['query_id']) == query_id and int(i[1]['fragment_id'])==fragment_id ]

    # group by operator name
    operators = defaultdict(list)
    for tp in tuples:       
        operators[tp[0]].append(tp[1]);
        
    # get fragment tree structure
    query_plan = read_json(query_plan_file)
    fragment = query_plan['fragments'][fragment_id]
    parent = get_parent(fragment)

    # update parent state
    induced_operators = defaultdict(list)
    for k,v in operators.items():
        if parent.has_key(k):
            for state in v:            
                if state['message']=='live':
                    state['message'] = 'wait'
                    induced_operators[parent[k]].append(state)
                elif state['message']=='hang':                 
                    state['message']= 'wake'
                    induced_operators[parent[k]].append(state)             

    for k,v in induced_operators.items():
        print k
        print v[0:3]   


def main(argv):
# Usage
    if len(argv) != 4:
        print >> sys.stderr, "Usage: %s <log_file_path> <query_id> <query_plan_file>" % (argv[0])
        print >> sys.stderr, "       log_file_path "
        print >> sys.stderr, "       query_id "
        print >> sys.stderr, "       query_plan_file "
        sys.exit(1)

    generateProfile(argv[1],int(argv[2]),1,argv[3])

if __name__ == "__main__":
    main(sys.argv)

