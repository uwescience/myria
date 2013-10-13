#!/usr/bin/env python

from collections import defaultdict
import itertools
import json
import sys

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
    fragment_id, a field id, and updating all the children links with the
    fragment id."""
    ret = []
    for (i, fragment) in enumerate(fragments):
        for operator in fragment['operators']:
            operator['fragment_id'] = i
            operator['id'] = str(i) + '-' + operator['op_name']
            for field in children[operator['op_type']]:
                names = []
                if isinstance(operator[field], list):
                    for child in operator[field]:
                        names.append(str(i) + '-' + child)
                else:
                    names.append(str(i) + '-' + operator[field])
                operator[field] = names         
            ret.append(operator)
    return ret

def operator_get_children(op):
    # Return the names of all child operators of this operator
    ret = []
    for x in children[op['op_type']]:
        for c in op[x]:
            ret.append(c)
    return ret

def operator_get_out_pipes(op):
    # By default, all operators have no out pipes
    pipe_fields = defaultdict(list)
    # Populate the list for all operators that do have children
    pipe_fields['CollectProducer'] = ['arg_operator_id']
    pipe_fields['EOSController'] = ['arg_idb_operator_ids']
    pipe_fields['LocalMultiwayProducer'] = ['arg_operator_ids']
    pipe_fields['ShuffleProducer'] = ['arg_operator_id']
    pipe_fields['IDBInput'] = ['arg_controller_operator_id']
    pipe_fields['BroadcastProducer'] = ['arg_operator_id']
    pipe_fields['HyperShuffleProducer'] = ['arg_operator_id']
    ret = []
    for x in pipe_fields[op['op_type']]:
        if isinstance(op[x],list):
            ret.extend([str(y) for y in op[x]])
        else:
            ret.append(str(op[x]))
    return ret

def operator_get_in_pipes(op):
    # By default, all operators have no in pipes
    pipe_fields = defaultdict(list)
    # Populate the list for all operators that do have children
    pipe_fields['CollectConsumer'] = ['arg_operator_id']
    pipe_fields['Consumer'] = ['arg_operator_id']
    pipe_fields['LocalMultiwayConsumer'] = ['arg_operator_id']
    pipe_fields['ShuffleConsumer'] = ['arg_operator_id']
    pipe_fields['BroadcastConsumer'] = ['arg_operator_id']
    pipe_fields['HyperShuffleConsumer'] = ['arg_operator_id']
    return [str(op[x]) for x in pipe_fields[op['op_type']]]

def get_graph(unified_plan):
    nodes = unified_plan
    local_edges = []
    in_pipes = defaultdict(list)
    out_pipes = defaultdict(list)
    for op in unified_plan:
        # Operator id
        op_id = op['id']
        # Add child edges
        local_edges.extend([(x,op_id) for x in operator_get_children(op)])
        # Add pipes
        for pipe_id in operator_get_in_pipes(op):
            in_pipes[pipe_id].append(op_id)
        for pipe_id in operator_get_out_pipes(op):
            out_pipes[pipe_id].append(op_id)
    pipe_edges = []
    for pipe_id in out_pipes:
        pipe_edges.extend([(x,y,pipe_id) for x in out_pipes[pipe_id] for y in in_pipes[pipe_id]])
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
       print "\"%s\" [label=\"%s\", color=%s, penwidth=2];" % (n['id'], n['op_name'], colors[n['fragment_id'] % len(colors)])
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
