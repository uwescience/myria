#!/usr/bin/env python

from collections import defaultdict
import itertools
import json
import sys

# By default, all operators have no children
children = defaultdict(list)
# Populate the list for all operators that do have children
children['CollectProducer'] = ['arg_child']
children['LocalJoin'] = ['arg_child1', 'arg_child2']
children['ShuffleProducer'] = ['arg_child']

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
                t[f] = str(fragment_id) + '-' + t[f]
            ret.append(t)
        return ret
    ret = [unify_fragment(x,y,z) for (x,y,z) in unique_fragments]
    return list(itertools.chain.from_iterable(ret))

def operator_get_children(op):
    # Return the names of all child operators of this operator
    return [op[x] for x in children[op['op_type']]]

def operator_get_out_pipes(op):
    # By default, all operators have no out pipes
    pipe_fields = defaultdict(list)
    # Populate the list for all operators that do have children
    pipe_fields['CollectProducer'] = ['arg_operator_id']
    pipe_fields['ShuffleProducer'] = ['arg_operator_id']
    return [str(op[x]) for x in pipe_fields[op['op_type']]]

def operator_get_in_pipes(op):
    # By default, all operators have no out pipes
    pipe_fields = defaultdict(list)
    # Populate the list for all operators that do have children
    pipe_fields['CollectConsumer'] = ['arg_operator_id']
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

def export_dot(nodes, edges, pipe_edges):
    print """digraph GlobalJoin {
  ratio = 1.333 ;
  mincross = 2.0 ;
  label = "Myria Global Join" ;
"""
    for n in nodes:
       print "\"%s\" [shape=box , regular=1, style=filled, fillcolor=white ] ;" % (n,)
    for (x,y) in edges:
        print "\"%s\" -> \"%s\" [weight=3, dir=back]" % (y, x)
    for (x,y) in pipe_edges:
        print "\"%s\" -> \"%s\" [weight=1, dir=back, penwidth=3]" % (y, x)
    print "}"

def main(filename):
    myria_json_plan = read_json(filename)
    query_plan = myria_json_plan['query_plan']
    unique_fragments = uniquify_fragments(query_plan)
    unified_plan = unify_plan(unique_fragments)
    [nodes, edges, pipe_edges] = get_graph(unified_plan)
    export_dot(nodes, edges, pipe_edges)

if __name__ == "__main__":
    main(sys.argv[1])
