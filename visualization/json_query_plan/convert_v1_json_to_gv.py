#!/usr/bin/env python

from collections import defaultdict
import itertools
import json
import sys

# By default, all operators have no children
children = defaultdict(list)
# Populate the list for all operators that do have children
children['CollectProducer'] = ['arg_child']
children['EOSController'] = ['arg_children']
children['IDBInput'] = ['arg_initial_input', 'arg_iteration_input', 'arg_eos_controller_input']
children['LocalJoin'] = ['arg_child1', 'arg_child2']
children['LocalMultiwayProducer'] = ['arg_child']
children['MultiGroupByAggregate'] = ['arg_child']
children['ShuffleProducer'] = ['arg_child']
children['SQLiteInsert'] = ['arg_child']
children['Aggregate'] = ['arg_child']
children['Apply'] = ['arg_child']
children['Filter'] = ['arg_child']
children['Project'] = ['arg_child']
children['LocalCountingJoin'] = ['arg_child1', 'arg_child2']
children['SQLiteInsert'] = ['arg_child']

def read_json(filename):
    with open(filename, 'r') as f:
        return json.load(f)

def uniquify_fragments(query_plan):
    fragment_inv = []
    for worker in sorted(query_plan.keys()):
        worker_plan = query_plan[worker]
        for fragment in worker_plan:
            flag = False
            for (i,(x,y)) in enumerate(fragment_inv):
                if y == fragment:
                    fragment_inv[i] = (x + [worker], y)
                    flag = True
                    break
            if flag:
                continue
            fragment_inv.append(([worker], fragment))
    return [(i,x,y) for (i,(x,y)) in enumerate(fragment_inv)]

def unify_plan(unique_fragments):
    def unify_fragment(fragment_id, workers, fragment):
        ret = []
        for op in fragment:
            t = op
            t['op_name'] = str(fragment_id) + '-' + t['op_name']
            for f in children[t['op_type']]:
                names = []
                if isinstance(t[f], list):
                    for child in t[f]:
                        names.append(str(fragment_id) + '-' + child)
                else:
                    names.append(str(fragment_id) + '-' + t[f])
                t[f] = names
            ret.append(t)
        return ret
    ret = [unify_fragment(x,y,z) for (x,y,z) in unique_fragments]
    return list(itertools.chain.from_iterable(ret))

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
    return [str(op[x]) for x in pipe_fields[op['op_type']]]

def get_graph(unified_plan):
    nodes = []
    local_edges = []
    in_pipes = defaultdict(list)
    out_pipes = defaultdict(list)
    for op in unified_plan:
        name = op['op_name']
        # New node
        nodes.append(name)
        # Add child edges
        local_edges.extend([(x,name) for x in operator_get_children(op)])
        # Add pipes
        for pipe_id in operator_get_in_pipes(op):
            in_pipes[pipe_id].append(name)
        for pipe_id in operator_get_out_pipes(op):
            out_pipes[pipe_id].append(name)
    pipe_edges = []
    for pipe_id in out_pipes:
        pipe_edges.extend([(x,y) for x in out_pipes[pipe_id] for y in in_pipes[pipe_id]])
    return (nodes, local_edges, pipe_edges)

def export_dot(nodes, edges, pipe_edges, filename=""):
    print """digraph MyriaPlan {
  ratio = 1.3333 ;
  mincross = 2.0 ;
  label = "Myria Plan for %s" ;
  rankdir = "BT" ;
  ranksep = 0.25 ;
  node [fontname="Helvetica", fontsize=10, shape=oval, style=filled, fillcolor=white ] ;
""" % (filename,)
    for n in nodes:
       print "\"%s\" ;" % (n,)
    for (x,y) in edges:
        print "\"%s\" -> \"%s\" [color=black]" % (x, y)
    for (x,y) in pipe_edges:
        print "\"%s\" -> \"%s\" [penwidth=5, color=red]" % (x, y)
    print "}"

def main(filename):
    myria_json_plan = read_json(filename)
    query_plan = myria_json_plan['query_plan']
    unique_fragments = uniquify_fragments(query_plan)
    unified_plan = unify_plan(unique_fragments)
    [nodes, edges, pipe_edges] = get_graph(unified_plan)
    export_dot(nodes, edges, pipe_edges, filename)

if __name__ == "__main__":
    main(sys.argv[1])
