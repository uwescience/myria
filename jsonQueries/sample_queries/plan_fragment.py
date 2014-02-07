#!/usr/bin/env python

import json

def pretty_json(obj):
    return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': '))

def scan_then_insert():
    query_scan = {
        'opType' : 'TableScan',
        'opName' : 'Scan',
        'relationKey' : {
            'userName' : 'jwang',
            'programName' : 'global_join',
            'relationName' : 'smallTable'
        },
    }

    insert = {
        'opType' : 'DbInsert',
        'opName' : 'Insert',
        'argChild' : 'Scan',
        'argOverwriteTable' : True,
        'relationKey' : {
            'userName' : 'jwang',
            'programName' : 'global_join',
            'relationName' : 'smallTable2'
        }
    }

    fragment = {
       'operators' : [query_scan, insert]
    }
    whole_plan = {
       'rawDatalog' : 'smallTable2(_) :- smallTable(_).',
       'logicalRa' : 'Insert[Scan[smallTable], smallTable2]',
       'fragments' : [fragment]
    }
    return whole_plan

def repartition_on_x():
    query_scan = {
        'opType' : 'TableScan',
        'opName' : 'Scan',
        'relationKey' : {
            'userName' : 'jwang',
            'programName' : 'global_join',
            'relationName' : 'smallTable'
        },
    }
    scatter = {
        'opType' : 'ShuffleProducer',
        'opName' : 'Scatter',
        'argChild' : 'Scan',
        'argOperatorId' : 'hash(follower)',
        'argPf' : {
            'type' : 'SingleFieldHash',
            'index' : 0
        }
    }
    gather = {
        'opType' : 'ShuffleConsumer',
        'opName' : 'Gather',
        'argOperatorId' : 'hash(follower)',
        "arg_schema" : {
            "columnTypes" : ["LONG_TYPE", "LONG_TYPE"],
            "columnNames" : ["follower", "followee"]
        }
    }
    insert = {
        'opType' : 'DbInsert',
        'opName' : 'Insert',
        'argChild' : 'Gather',
        'argOverwriteTable' : True,
        'relationKey' : {
            'userName' : 'jwang',
            'programName' : 'global_join',
            'relationName' : 'smallTable_hash_follower'
        }
    }

    fragment1 = {
       'operators' : [query_scan, scatter]
    }
    fragment2 = {
       'operators' : [gather, insert]
    }
    whole_plan = {
       'rawDatalog' : 'smallTable_hash_follower(x,y) :- smallTable(x,y), @hash(x).',
       'logicalRa' : 'Insert[Shuffle(0)[Scan[smallTable], smallTable2]]',
       'fragments' : [fragment1, fragment2]
    }
    return whole_plan

def single_join():
    scan0 = {
        'opType' : 'TableScan',
        'opName' : 'Scan0',
        'relationKey' : {
            'userName' : 'jwang',
            'programName' : 'global_join',
            'relationName' : 'smallTable'
        },
    }
    scatter0 = {
        'opType' : 'ShuffleProducer',
        'opName' : 'Scatter0',
        'argChild' : 'Scan0',
        'argOperatorId' : 'hash(x)',
        'argPf' : {
            'type' : 'SingleFieldHash',
            'index' : 0
        }
    }
    gather0 = {
        'opType' : 'ShuffleConsumer',
        'opName' : 'Gather0',
        'argOperatorId' : 'hash(x)',
        "arg_schema" : {
            "columnTypes" : ["LONG_TYPE", "LONG_TYPE"],
            "columnNames" : ["follower", "followee"]
        }
    }

    scan1 = {
        'opType' : 'TableScan',
        'opName' : 'Scan1',
        'relationKey' : {
            'userName' : 'jwang',
            'programName' : 'global_join',
            'relationName' : 'smallTable'
        },
    }
    scatter1 = {
        'opType' : 'ShuffleProducer',
        'opName' : 'Scatter1',
        'argChild' : 'Scan1',
        'argOperatorId' : 'hash(y)',
        'argPf' : {
            'type' : 'SingleFieldHash',
            'index' : 1
        }
    }
    gather1 = {
        'opType' : 'ShuffleConsumer',
        'opName' : 'Gather1',
        'argOperatorId' : 'hash(y)',
        "arg_schema" : {
            "columnTypes" : ["LONG_TYPE", "LONG_TYPE"],
            "columnNames" : ["follower", "followee"]
        }
    }

    join = {
        'opType' : 'SymmetricHashJoin',
        'opName' : 'Join',
        'argChild1' : 'Gather1',
        'argChild2' : 'Gather0',
        'arg_columns1' : [1],
        'arg_columns2' : [0],
        'arg_select1' : [0],
        'arg_select2' : [1],
    }
    insert = {
        'opType' : 'DbInsert',
        'opName' : 'Insert',
        'argChild' : 'Join',
        'argOverwriteTable' : True,
        'relationKey' : {
            'userName' : 'jwang',
            'programName' : 'global_join',
            'relationName' : 'smallTable_join_smallTable'
        }
    }

    fragmentLeft = {
        'operators' : [scan0, scatter0],
    }
    fragmentRight = {
        'operators' : [scan1, scatter1],
    }
    fragmentJoin = {
        'operators' : [gather0, gather1, join, insert],
    }
    whole_plan = {
       'rawDatalog' : 'smallTable_join_smallTable(x,z) :- smallTable(x,y), mallTable(y,z)',
       'logicalRa' : 'Insert(smallTable_join_smallTable)[Join(1=0; [0,3])[Shuffle(1)[Scan], Shuffle(1)[Scan]]]',
       'fragments' : [fragmentLeft, fragmentRight, fragmentJoin]
    }
    return whole_plan

def tipsy_schema():
    return {
        "columnTypes": [
            "LONG_TYPE", "FLOAT_TYPE", "FLOAT_TYPE", "FLOAT_TYPE",
            "FLOAT_TYPE", "FLOAT_TYPE", "FLOAT_TYPE", "FLOAT_TYPE",
            "FLOAT_TYPE", "FLOAT_TYPE", "FLOAT_TYPE", "FLOAT_TYPE",
            "FLOAT_TYPE", "FLOAT_TYPE", "FLOAT_TYPE", "INT_TYPE", "STRING_TYPE"
        ],
        "columnNames": [
            "iOrder", "mass", "x", "y", "z", "vx", "vy", "vz", "rho", "temp",
            "hsmooth", "metals", "tform", "eps", "phi", "grp", "type"
        ]
    }

def ingest_tipsy_rr():
    BASE_FILE = '/Users/dhalperi/escience/myria/data_nocommit/tipsy/'
    BASE_FILE = '/disk2/dhalperi'
    tipsy_scan = {
        "opType" : 'TipsyFileScan',
        'opName' : 'Scan',
        "tipsyFilename": BASE_FILE+"/cosmo50cmb.256g2MbwK.00512",
        "iorderFilename": BASE_FILE+"/cosmo50cmb.256g2MbwK.00512.iord",
        "grpFilename": BASE_FILE+"/cosmo50cmb.256g2MbwK.00512.amiga.grp"
    }
    scatter = {
        'opType' : 'ShuffleProducer',
        'opName' : 'Scatter',
        'argChild' : 'Scan',
        'argOperatorId' : 'RoundRobin',
        'argPf' : {
            'type' : 'RoundRobin'
        }
    }
    scan_fragment = {
        'operators' : [ tipsy_scan, scatter ],
    }

    gather = {
        'opType' : 'ShuffleConsumer',
        'opName' : 'Gather',
        'argOperatorId' : 'RoundRobin',
        "arg_schema" : tipsy_schema()
    }
    insert = {
        'opType' : 'DbInsert',
        'opName' : 'Insert',
        'argChild' : 'Gather',
        'argOverwriteTable' : True,
        'relationKey' : {
            'userName' : 'leelee',
            'programName' : 'astro',
            'relationName' : 'cosmo512'
        }
    }
    insert_fragment = {
        'operators' : [ gather, insert ]
    }

    return {
        'logicalRa' : 'ingest tipsy rr',
        'rawDatalog' : 'ingest tipsy rr',
        'fragments' : [ scan_fragment, insert_fragment ]
    }

def ingest_tipsy_hash_iorder():
    BASE_FILE = '/Users/dhalperi/escience/myria/data_nocommit/tipsy/'
    BASE_FILE = '/disk2/dhalperi'
    tipsy_scan = {
        "opType" : 'TipsyFileScan',
        'opName' : 'Scan',
        "tipsyFilename": BASE_FILE+"/cosmo50cmb.256g2MbwK.00512",
        "iorderFilename": BASE_FILE+"/cosmo50cmb.256g2MbwK.00512.iord",
        "grpFilename": BASE_FILE+"/cosmo50cmb.256g2MbwK.00512.amiga.grp"
    }
    scatter = {
        'opType' : 'ShuffleProducer',
        'opName' : 'Scatter',
        'argChild' : 'Scan',
        'argOperatorId' : 'hash(iorder)',
        'argPf' : {
            'type' : 'SingleFieldHash',
            'index' : 0
        }
    }
    scan_fragment = {
        'operators' : [ tipsy_scan, scatter ],
    }

    gather = {
        'opType' : 'ShuffleConsumer',
        'opName' : 'Gather',
        'argOperatorId' : 'hash(iorder)',
        "arg_schema" : tipsy_schema()
    }
    insert = {
        'opType' : 'DbInsert',
        'opName' : 'Insert',
        'argChild' : 'Gather',
        'argOverwriteTable' : True,
        'relationKey' : {
            'userName' : 'leelee',
            'programName' : 'astro',
            'relationName' : 'cosmo512'
        }
    }
    insert_fragment = {
        'operators' : [ gather, insert ]
    }

    return {
        'logicalRa' : 'ingest tipsy rr',
        'rawDatalog' : 'ingest tipsy rr',
        'fragments' : [ scan_fragment, insert_fragment ]
    }

#print pretty_json(repartition_on_x())
print pretty_json(single_join())
#print pretty_json(ingest_tipsy_hash_iorder())
