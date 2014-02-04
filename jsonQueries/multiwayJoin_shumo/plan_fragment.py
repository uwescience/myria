#!/usr/bin/env python

import json

def pretty_json(obj):
    return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ':'))

def twitter_small_relation_key():
    relation_key = {
        "userName" : "chushumo",
        "programName" : "multiway_join",
        "relationName" : "twitter_small"
    }
    return relation_key


def scan_R_then_shuffle():
    scan = {
        "opType" : "TableScan",
        "opName" : "Scan(R)",
        "relationKey" : twitter_small_relation_key()
    }

    hyper_shuffle = {
        "opType" : "HyperShuffleProducer",
        "opName" : "HyperShuffle(R)",
        "argChild" : "Scan(R)",
        "argOperatorId" : "hash(follower)",
        "fieldIndexes" : [0],
        "hyperCubeDimensions" : [2,2],
        "cellPartition": [ [0,1],[2,3] ]           
    }

    fragment = {
       "operators" : [scan, hyper_shuffle]
    }
    
    return fragment

def scan_S_then_shuffle():
    scan = {
        "opType" : "TableScan",
        "opName" : "Scan(S)",
        "relationKey" : twitter_small_relation_key()
    }

    hyper_shuffle = {
        "opType" : "HyperShuffleProducer",
        "opName" : "HyperShuffle(S)",
        "argChild" : "Scan(S)",
        "argOperatorId" : "hash(followee)",
        "fieldIndexes" : [1],
        "hyperCubeDimensions" : [2,2],
        "cellPartition": [ [0,2],[1,3] ]           
    }
    fragment = {
        "operators" : [scan, hyper_shuffle]
    }
    return fragment

def receive_then_join():
    gatherR = {
        "opType" : "HyperShuffleConsumer",
        "opName" : "GatherR",
        "argOperatorId" : "hash(follower)",
        "arg_schema" : {
            "columnTypes" : ["LONG_TYPE", "LONG_TYPE"],
            "columnNames" : ["follower", "followee"]
        }
    }
    gatherS = {
        "opType" : "HyperShuffleConsumer",
        "opName" : "GatherS",
        "argOperatorId" : "hash(followee)",
        "arg_schema" : {
            "columnTypes" : ["LONG_TYPE", "LONG_TYPE"],
            "columnNames" : ["follower", "followee"]
        }
    }
    join = {
        "opType" : "SymmetricHashJoin",
        "opName" : "Join",
        "argChild1" : "GatherR",
        "argChild2" : "GatherS",
        "arg_columns1" : [1],
        "arg_columns2" : [0],
        "arg_select1" : [0],
        "arg_select2" : [1]
    }    
    collect = {
        "argChild": "Join",
        "argOperatorId": "collect",
        "opName": "SendResult",
        "opType": "CollectProducer"
    }
    fragment = {
        "operators": [gatherR, gatherS, join, collect]
    }
    return fragment

def collect_result():
    gather = {
        "argOperatorId": "collect",
        "arg_schema": {
            "columnNames": [
                "follower",
                "followee"
            ],
            "columnTypes": [
                "LONG_TYPE",
                "LONG_TYPE"
            ]
        },
        "opName": "CollectResult",
        "opType": "CollectConsumer"                       
    }
    insert = {
        "argChild": "CollectResult",
        "argOverwriteTable": True,
        "opName": "Insert",
        "opType": "DbInsert",
        "relationKey": {
            "programName": "multiway_join",
            "relationName": "twitter_small_join_twitter_small",
            "userName": "chushumo"
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
        "logicalRa" : "two dimension multiway join",
        "rawDatalog" : "two dimension multiway join"
    }
    return whole_plan

def scan_R_then_partition():
    scan = {
        "opType" : "TableScan",
        "opName" : "Scan(R)",
        "relationKey" : twitter_small_relation_key()
    }
    shuffle = {
        "opType" : "ShuffleProducer",
        "opName" : "Shuffle(R)",
        "argChild" : "Scan(R)",
        "argOperatorId" : "hash(followee)",
        "argPf" : 
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
        "opType" : "TableScan",
        "opName" : "Scan(S)",
        "relationKey" : twitter_small_relation_key()
    }

    shuffle = {
        "opType" : "ShuffleProducer",
        "opName" : "Shuffle(S)",
        "argChild" : "Scan(S)",
        "argOperatorId" : "hash(follower)",
        "argPf" : 
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
        "opType" : "ShuffleConsumer",
        "opName" : "GatherR",
        "argOperatorId" : "hash(followee)",
        "arg_schema" : {
            "columnTypes" : ["LONG_TYPE", "LONG_TYPE"],
            "columnNames" : ["follower", "followee"]
        }
    }
    gatherS = {
        "opType" : "ShuffleConsumer",
        "opName" : "GatherS",
        "argOperatorId" : "hash(follower)",
        "arg_schema" : {
            "columnTypes" : ["LONG_TYPE", "LONG_TYPE"],
            "columnNames" : ["follower", "followee"]
        }
    }
    join = {
        "opType" : "SymmetricHashJoin",
        "opName" : "Join",
        "argChild1" : "GatherR",
        "argChild2" : "GatherS",
        "arg_columns1" : [1],
        "arg_columns2" : [0],
        "arg_select1" : [0],
        "arg_select2" : [1]
    }    
    collect = {
        "argChild": "Join",
        "argOperatorId": "collect",
        "opName": "SendResult",
        "opType": "CollectProducer"
    }
    fragment = {
        "operators": [gatherR, gatherS, join, collect]
    }
    return fragment

def collect_partition_join_result():
    gather = {
        "argOperatorId": "collect",
        "arg_schema": {
            "columnNames": [
                "follower",
                "followee"
            ],
            "columnTypes": [
                "LONG_TYPE",
                "LONG_TYPE"
            ]
        },
        "opName": "CollectResult",
        "opType": "CollectConsumer"                       
    }
    insert = {
        "argChild": "CollectResult",
        "argOverwriteTable": True,
        "opName": "Insert",
        "opType": "DbInsert",
        "relationKey": {
            "programName": "multiway_join",
            "relationName": "twitter_small_partition_join_twitter_small",
            "userName": "chushumo"
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
        "logicalRa" : "partition join",
        "rawDatalog" : "parittion join"
    }
    return whole_plan


#print pretty_json(two_dimension_multiway_join())
print pretty_json(partition_join())
