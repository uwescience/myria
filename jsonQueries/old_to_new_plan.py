#!/usr/bin/env python

import json
import sys

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
    return fragment_inv

def json_pretty(obj):
    return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': '))

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: %s <old json file>" % sys.argv[0]
        sys.exit(1)

    myria_json_plan = read_json(sys.argv[1])
    fragments = []
    frags = uniquify_fragments(myria_json_plan['query_plan'])
    for (ws,ops) in frags:
        fragments.append({
            'workers' : ws,
            'operators' : ops
        })
    output = {
            'raw_datalog' : myria_json_plan['raw_datalog'],
            'logical_ra' : myria_json_plan['logical_ra'],
            'fragments' : fragments
    }

    print json_pretty(output)
