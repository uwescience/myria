import json

output_file = "query.json"
schema_config_file = "schema_config.json"
input_config_file = "input_config.json"
op_tree_file = "op_tree.json"

# load schemas
with open(schema_config_file) as input_file:
    schema_config = json.load(input_file)

# load input configuration
with open(input_config_file) as input_file:
    input_config = json.load(input_file)

# load operator tree
with open(op_tree_file) as input_file:
    op_tree = json.load(input_file)

# find the max opId
max_id = 0
for fragment in op_tree['fragments']:
    for op in fragment['operators']:
        max_id = max(max_id, op['opId'])

# ASSUMPTION: the input opId starts at 1

# number of workers
inputs = input_config['inputs']
worker_num = len(inputs)

query = {}

op_idx = max_id


def next_op_id():
    global op_idx
    op_idx += 1
    return op_idx

# Final output of file scans
prod_op_id = 0


def next_prod_op_id():
    global prod_op_id
    prod_op_id += 1
    return prod_op_id

fragments = {}
fragments["fragments"] = []

shuffle_prods = {}

# add fragments for scanning files
for i in range(0, worker_num):
    for relation, sources in inputs[i].iteritems():
        fragment = {}
        # fragment for ith worker on relation
        fragment['overrideWorkers'] = i

        # ops: FileScan -> UnionAll -> ShuffleProducer
        ops = []

        # UnionAll
        op_ua = {}
        op_ua['opType'] = 'UnionAll'
        op_ua['opId'] = next_op_id()
        op_ua['argChildren'] = []

        # ShuffleProducer
        op_sp = {}
        op_sp['opType'] = 'ShuffleProducer'
        op_sp['opId'] = next_op_id()
        op_sp['argChild'] = op_ua['opId']
        arg_pf = {}
        arg_pf['index'] = 0
        arg_pf['type'] = "SingleFieldHash"
        op_sp['argPf'] = arg_pf

        # update producer list
        if relation not in shuffle_prods:
            shuffle_prods[relation] = []
        shuffle_prods[relation].append(op['opId'])

        # add FileScan operators
        for source in sources:
            op = {}
            op['opType'] = "FileScan"
            op['opId'] = next_op_id()
            op['source'] = source
            op['schema'] = schema_config['schemas'][relation]

            op_ua['argChildren'].append(op['opId'])
            ops.append(op)
        ops.append(op_ua)
        ops.append(op_sp)
        # add operators
        fragment['operators'] = ops
        # add fragment
        fragments['fragments'].append(fragment)

# add fragments for shuffle relations
for relation, prods in shuffle_prods.iteritems():
    fragment = {}
    ops = []
    # ops: ShuffleConsumer -> UnionAll -> ShuffleProducer

    # UnionAll
    op_ua = {}
    op_ua['opType'] = 'UnionAll'
    op_ua['opId'] = next_op_id()
    op_ua['argChildren'] = []

    # ShuffleProducer
    op_sp = {}
    op_sp['opType'] = 'ShuffleProducer'
    op_sp['opId'] = next_prod_op_id()
    op_sp['argChild'] = op_ua['opId']
    arg_pf = {}
    arg_pf['index'] = 0
    arg_pf['type'] = "SingleFieldHash"
    op_sp['argPf'] = arg_pf

    # ShuffleConsumer
    for prod_id in prods:
        op = {}
        op['opType'] = "ShuffleConsumer"
        op['opId'] = next_op_id()
        op['argOperatorId'] = prod_id

        op_ua['argChildren'].append(op['opId'])
        ops.append(op)

    ops.append(op_ua)
    ops.append(op_sp)
    fragment['operators'] = ops
    fragments['fragments'].append(fragment)


# add fragments from op tree
fragments['fragments'].extend(op_tree["fragments"])
outfile = open(output_file, "w")
json.dump(fragments, outfile, sort_keys=True, indent=4, separators=(',', ': '))
