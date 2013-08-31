#!/usr/bin/env python

import json

def pretty_json(obj):
    return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ':'))

def twitter_small_relation_key():
    relation_key = {
        "user_name" : "chushumo",
        "program_name" : "multiway_join",
        "relation_name" : "twitter_small"
    }
    return relation_key


def scan_R_then_shuffle():
    scan = {
        "op_type" : "TableScan",
        "op_name" : "Scan(R)",
        "relation_key" : twitter_small_relation_key()
    }

    hyper_shuffle = {
        "op_type" : "HyperShuffleProducer",
        "op_name" : "HyperShuffle(R)",
        "arg_child" : "Scan(R)",
        "arg_operator_id" : "hash(follower)",
        "arg_pfs" : [
            {
                "type" : "SingleFieldHash",
                "index" : 0
            }
        ],
        "hyper_cube_dimensions" : [2,2],
        "cell_partition": [ [0,1],[2,3] ]           
    }

    fragment = {
       "operators" : [scan, hyper_shuffle]
    }
    
    return fragment

def scan_S_then_shuffle():
    scan = {
        "op_type" : "TableScan",
        "op_name" : "Scan(S)",
        "relation_key" : twitter_small_relation_key()
    }

    hyper_shuffle = {
        "op_type" : "HyperShuffleProducer",
        "op_name" : "HyperShuffle(S)",
        "arg_child" : "Scan(S)",
        "arg_operator_id" : "hash(followee)",
        "arg_pfs" : [
            {
                "type" : "SingleFieldHash",
                "index" : 1
            }
        ],
        "hyper_cube_dimensions" : [2,2],
        "cell_partition": [ [0,2],[1,3] ]           
    }
    fragment = {
        "operators" : [scan, hyper_shuffle]
    }
    return fragment

def receive_then_join():
    gatherR = {
        "op_type" : "HyperShuffleConsumer",
        "op_name" : "GatherR",
        "arg_operator_id" : "hash(follower)",
        "arg_schema" : {
            "column_types" : ["LONG_TYPE", "LONG_TYPE"],
            "column_names" : ["follower", "followee"]
        }
    }
    gatherS = {
        "op_type" : "HyperShuffleConsumer",
        "op_name" : "GatherS",
        "arg_operator_id" : "hash(followee)",
        "arg_schema" : {
            "column_types" : ["LONG_TYPE", "LONG_TYPE"],
            "column_names" : ["follower", "followee"]
        }
    }
    localJoin = {
        "op_type" : "LocalJoin",
        "op_name" : "Join",
        "arg_child1" : "GatherR",
        "arg_child2" : "GatherS",
        "arg_columns1" : [1],
        "arg_columns2" : [0],
        "arg_select1" : [0],
        "arg_select2" : [1]
    }    
    collect = {
        "arg_child": "Join",
        "arg_operator_id": "collect",
        "op_name": "SendResult",
        "op_type": "CollectProducer"
    }
    fragment = {
        "operators": [gatherR, gatherS, localJoin, collect]
    }
    return fragment

def collect_result():
    gather = {
        "arg_operator_id": "collect",
        "arg_schema": {
            "column_names": [
                "follower",
                "followee"
            ],
            "column_types": [
                "LONG_TYPE",
                "LONG_TYPE"
            ]
        },
        "op_name": "CollectResult",
        "op_type": "CollectConsumer"                       
    }
    insert = {
        "arg_child": "CollectResult",
        "arg_overwrite_table": True,
        "op_name": "Insert",
        "op_type": "DbInsert",
        "relation_key": {
            "program_name": "multiway_join",
            "relation_name": "twitter_small_join_twitter_small",
            "user_name": "chushumo"
        }
    }
    fragment = {
        "operators": [ gather, insert],
        "workers" : [ 1 ]
    }
    return fragment

def two_dimension_multiway_join():
    fragments = [scan_R_then_shuffle(), scan_S_then_shuffle(), receive_then_join(), collect_result()]
    whole_plan = {
        "fragments":fragments,
        "logical_ra" : "two dimension multiway join",
        "raw_datalog" : "two dimension multiway join"
    }
    return whole_plan

def scan_R_then_partition():
    scan = {
        "op_type" : "TableScan",
        "op_name" : "Scan(R)",
        "relation_key" : twitter_small_relation_key()
    }
    shuffle = {
        "op_type" : "ShuffleProducer",
        "op_name" : "Shuffle(R)",
        "arg_child" : "Scan(R)",
        "arg_operator_id" : "hash(followee)",
        "arg_pf" : 
            {
                "type" : "SingleFieldHash",
                "index" : 1
            }        
    }  
    fragment = {
       "operators" : [scan, shuffle]
    }
    
    return fragment

def scan_S_then_partition():
    scan = {
        "op_type" : "TableScan",
        "op_name" : "Scan(S)",
        "relation_key" : twitter_small_relation_key()
    }

    shuffle = {
        "op_type" : "ShuffleProducer",
        "op_name" : "Shuffle(S)",
        "arg_child" : "Scan(S)",
        "arg_operator_id" : "hash(follower)",
        "arg_pf" : 
            {
                "type" : "SingleFieldHash",
                "index" : 0
            }        
    }
    fragment = {
        "operators" : [scan, shuffle]
    }
    return fragment

def receive_partition_then_join():
    gatherR = {
        "op_type" : "ShuffleConsumer",
        "op_name" : "GatherR",
        "arg_operator_id" : "hash(followee)",
        "arg_schema" : {
            "column_types" : ["LONG_TYPE", "LONG_TYPE"],
            "column_names" : ["follower", "followee"]
        }
    }
    gatherS = {
        "op_type" : "ShuffleConsumer",
        "op_name" : "GatherS",
        "arg_operator_id" : "hash(follower)",
        "arg_schema" : {
            "column_types" : ["LONG_TYPE", "LONG_TYPE"],
            "column_names" : ["follower", "followee"]
        }
    }
    localJoin = {
        "op_type" : "LocalJoin",
        "op_name" : "Join",
        "arg_child1" : "GatherR",
        "arg_child2" : "GatherS",
        "arg_columns1" : [1],
        "arg_columns2" : [0],
        "arg_select1" : [0],
        "arg_select2" : [1]
    }    
    collect = {
        "arg_child": "Join",
        "arg_operator_id": "collect",
        "op_name": "SendResult",
        "op_type": "CollectProducer"
    }
    fragment = {
        "operators": [gatherR, gatherS, localJoin, collect]
    }
    return fragment

def collect_partition_join_result():
    gather = {
        "arg_operator_id": "collect",
        "arg_schema": {
            "column_names": [
                "follower",
                "followee"
            ],
            "column_types": [
                "LONG_TYPE",
                "LONG_TYPE"
            ]
        },
        "op_name": "CollectResult",
        "op_type": "CollectConsumer"                       
    }
    insert = {
        "arg_child": "CollectResult",
        "arg_overwrite_table": True,
        "op_name": "Insert",
        "op_type": "DbInsert",
        "relation_key": {
            "program_name": "multiway_join",
            "relation_name": "twitter_small_partition_join_twitter_small",
            "user_name": "chushumo"
        }
    }
    fragment = {
        "operators": [gather, insert],
        "workers" : [ 1 ]
    }
    return fragment

# as a baseline to validate
def partition_join():
    fragments = [scan_R_then_partition(), scan_S_then_partition(), receive_partition_then_join(), collect_partition_join_result()]
    whole_plan = {
        "fragments":fragments,
        "logical_ra" : "partition join",
        "raw_datalog" : "parittion join"
    }
    return whole_plan


#print pretty_json(two_dimension_multiway_join())
print pretty_json(partition_join())
