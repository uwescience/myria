#!/usr/bin/env python

from collections import defaultdict
import json
import sys

# Fields to delete for certain operators
# .. these are fields that are automatically inferred now.
delete_fields = defaultdict(list)
delete_fields['Consumer'] = ['arg_worker_ids']
delete_fields['CollectConsumer'] = ['arg_worker_ids']
delete_fields['CollectProducer'] = ['arg_worker_id']
delete_fields['EOSController'] = ['arg_worker_ids']
delete_fields['IDBController'] = ['arg_controller_worker_id']
delete_fields['ShuffleConsumer'] = ['arg_worker_ids']
delete_fields['ShuffleProducer'] = ['arg_worker_ids']

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

def clean_up(ops):
    def _clean_op(op):
        for field in delete_fields[op['opType']]:
            try:
                del op[field]
            except:
                pass
        return op
    return [_clean_op(op) for op in ops]

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
            'overrideWorkers' : ws,
            'operators' : clean_up(ops)
        })
    output = {
            'rawQuery' : myria_json_plan['rawQuery'],
            'logicalRa' : myria_json_plan['logicalRa'],
            'fragments' : fragments
    }

    print json_pretty(output)
