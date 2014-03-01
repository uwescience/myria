#!/usr/bin/env python

from collections import defaultdict
import itertools
import json
import sys

# By default, all operators have no children
children = defaultdict(list)
# Populate the list for all operators that do have children
children['CollectProducer'] = ['argChild']
children['EOSController'] = ['argChild']
children['IDBInput'] = ['argInitialInput', 'argIterationInput', 'argEosControllerInput']
children['RightHashJoin'] = ['argChild1', 'argChild2']
children['RightHashCountingJoin'] = ['argChild1', 'argChild2']
children['SymmetricHashJoin'] = ['argChild1', 'argChild2']
children['LocalMultiwayProducer'] = ['argChild']
children['MultiGroupByAggregate'] = ['argChild']
children['SingleGroupByAggregate'] = ['argChild']
children['ShuffleProducer'] = ['argChild']
children['DbInsert'] = ['argChild']
children['Aggregate'] = ['argChild']
children['Apply'] = ['argChild']
children['Filter'] = ['argChild']
children['UnionAll'] = ['argChildren']
children['Merge'] = ['argChildren']
children['LeapFrogJoin'] = ['argChildren']
children['ColumnSelect'] = ['argChild']
children['SymmetricHashCountingJoin'] = ['argChild1', 'argChild2']
children['BroadcastProducer'] = ['argChild']
children['HyperShuffleProducer'] = ['argChild']
children['InMemoryOrderBy'] = ['argChild']
children['SinkRoot'] = ['argChild']
children['DupElim'] = ['argChild']
children['Rename'] = ['argChild']

# Colors supported by graphviz, in some pleasing order
colors = [
        "red",      #0
        "green",    #1
        "blue",     #2
        "yellow",   #3
        "purple",   #4
        "orange",   #5
        "cyan",     #6
        "magenta"   #7
]

def read_json(filename):
    with open(filename, 'r') as f:
        return json.load(f)

def unify_fragments(fragments):
    """Returns a list of operators, adding to each operator a field
    fragment_id, a field id, and converting all the children links to
    list type."""
    ret = []
    for (i, fragment) in enumerate(fragments):
        for operator in fragment['operators']:
            operator['fragmentId'] = i
            for field in children[operator['opType']]:
                if not isinstance(operator[field], list):
                    operator[field] = [operator[field]]        
            ret.append(operator)
    return ret

def operator_get_children(op):
    # Return the names of all child operators of this operator
    ret = []
    for x in children[op['opType']]:
        for c in op[x]:
            ret.append(c)
    return ret

def operator_get_out_pipes(op):
    # By default, all operators have no out pipes
    pipe_fields = defaultdict(list)
    # Populate the list for all operators that do have children
    pipe_fields['CollectProducer'] = ['argOperatorId']
    pipe_fields['EOSController'] = ['arg_idb_operator_ids']
    pipe_fields['LocalMultiwayProducer'] = ['arg_operator_ids']
    pipe_fields['ShuffleProducer'] = ['argOperatorId']
    pipe_fields['IDBInput'] = ['arg_controller_operator_id']
    pipe_fields['BroadcastProducer'] = ['argOperatorId']
    pipe_fields['HyperShuffleProducer'] = ['argOperatorId']
    ret = []
    for x in pipe_fields[op['opType']]:
        if isinstance(op[x],list):
            ret.extend([str(y) for y in op[x]])
        else:
            ret.append(str(op[x]))
    return ret

def operator_get_in_pipes(op):
    # By default, all operators have no in pipes
    pipe_fields = defaultdict(list)
    # Populate the list for all operators that do have children
    pipe_fields['CollectConsumer'] = ['argOperatorId']
    pipe_fields['Consumer'] = ['argOperatorId']
    pipe_fields['LocalMultiwayConsumer'] = ['argOperatorId']
    pipe_fields['ShuffleConsumer'] = ['argOperatorId']
    pipe_fields['BroadcastConsumer'] = ['argOperatorId']
    pipe_fields['HyperShuffleConsumer'] = ['argOperatorId']
    return [str(op[x]) for x in pipe_fields[op['opType']]]

def get_graph(unified_plan):
    nodes = unified_plan
    local_edges = []
    in_pipes = defaultdict(list)
    for op in unified_plan:
        # Operator id
        op_id = op['opName']
        # Add child edges
        local_edges.extend([(x,op_id) for x in operator_get_children(op)])
        # Add pipes
        for producing_op_id in operator_get_in_pipes(op):
            in_pipes[producing_op_id].append(op_id)
        #for pipe_id in operator_get_out_pipes(op):
        #    out_pipes[pipe_id].append(op_id)
    pipe_edges = []
    for producing_op_id in in_pipes:
        pipe_edges.extend([(producing_op_id,y,"") for y in in_pipes[producing_op_id]])
    return (unified_plan, local_edges, pipe_edges)

def export_dot(nodes, edges, pipe_edges, filename=""):
    print """digraph MyriaPlan {
  ratio = 1.3333 ;
  mincross = 2.0 ;
  label = "Myria Plan for %s" ;
  rankdir = "BT" ;
  ranksep = 0.25 ;
  node [fontname="Helvetica", fontsize=10, shape=oval, style=filled, fillcolor=white ] ;
  edge [fontname="Helvetica", fontsize=9 ] ;
""" % (filename,)
    for n in nodes:
       print "\"%s\" [label=\"%s\", color=%s, penwidth=2];" % (n['opName'], n['opName'], colors[n['fragmentId'] % len(colors)])
    for (x,y) in edges:
        print "\"%s\" -> \"%s\" [color=black]" % (x, y)
    for (x,y,label) in pipe_edges:
        print "\"%s\" -> \"%s\" [penwidth=3, style=dashed, label=\"    %s\"]" % (x, y, label)
    print "}"

def main(filename):
    myria_json_plan = read_json(filename)
    fragments = myria_json_plan['fragments']
    unified_plan = unify_fragments(fragments)
    [nodes, edges, pipe_edges] = get_graph(unified_plan)
    export_dot(nodes, edges, pipe_edges, filename)

if __name__ == "__main__":
    main(sys.argv[1])
