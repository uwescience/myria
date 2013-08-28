#!/usr/bin/env python

import json

def pretty_json(obj):
    return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': '))

def connection_info(host):
    connection_info = {
        "driver_class": "com.vertica.jdbc.Driver",
        "dbms": "vertica",
        "host": host,
        "port": "15433",
        "username": "dbadmin",
        "database": "mrbenchmarks",
        "password": "mrbenchmarks"
    }

    return connection_info

def scan_broadcast(relation_key):
    
    scan = {
        "op_name": "ScanR",
        "op_type": "TableScan",
        "relation_key": relation_key,
        "connection_info": connection_info("localhost")
    }
    
    broadcast = {
        "arg_child": "ScanR",
        "arg_operator_id": "broadcast",
        "op_name": "broadcast",
        "op_type": "BroadcastProducer"
    }

    fragment = {
        "operators": [ scan, broadcast ]
    }

    return fragment

def scan_join(relation_key):
    scan = {
        "op_name": "ScanU",
        "op_type": "TableScan",
        "relation_key": {
            "program_name": "broadcastjoin",
            "relation_name": "UserBase",
            "user_name": "shumochu"
        },
        "connection_info":connection_info("localhost")
    }
    broadcast_consumer = {
        "arg_operator_id": "broadcast",
        "arg_schema": {
            "column_types" : ["LONG_TYPE","STRING_TYPE","LONG_TYPE"],
            "column_names" : ["pageRank","pageURL","avgDuration"]
        },
        "op_name": "receive",
        "op_type": "BroadcastConsumer"
    }
    join = {
        "arg_child1": "receive",
        "arg_child2": "ScanU",
        "arg_columns1": [ 1 ],
        "arg_columns2": [ 1 ],
        "arg_select1": [ 0 ],
        "arg_select2": [ 1 ],                
        "op_name": "Join",
        "op_type": "LocalJoin"
    }
    insert = {
        "arg_child": "Join",
        "op_name": "Insert",
        "op_type": "DbInsert",
        "relation_key": relation_key,
        "arg_overwrite_table": True,
        "connection_info": ("localhost")
    }
    fragment = {
        "operators": [ scan, broadcast_consumer, join, insert ]
    }

    return fragment

def generate_broadcastjoin():
    
    table_R_key = {
        "program_name": "broadcastjoin",
        "relation_name": "RankBase2x",
        "user_name": "shumochu"
    }
    result_key = {
        "program_name": "broadcastjoin",
        "relation_name": "result2x",
        "user_name": "shumochu"
    }
    
    fragments = [ scan_broadcast(table_R_key), scan_join(result_key) ]

    whole_plan = {
        "fragments": fragments,
        "logical_ra": "broadcast join",
        "raw_datalog": "broadcast join"
    }

    return whole_plan

def scan_and_shuffle(relation_key, relation_name):
    scan = {
        "op_name": "Scan("+relation_name+")",
        "op_type": "TableScan",
        "relation_key": relation_key,
        "connection_info": connection_info("localhost")
    }
    shuffle = {
        "arg_child": "Scan("+relation_name+")",
        "arg_operator_id": "Shuffle("+relation_name+")",
        "op_name": "Shuffle("+relation_name+")",
        "op_type": "ShuffleProducer",
        "arg_pf": {
            "index": 1,
            "type": "SingleFieldHash"
        }
    }
    fragment = {
        "operators": [scan, shuffle ]       
    }

    return fragment

def receive_and_join(relation_key):
    gatherR = {
        "arg_operator_id": "Shuffle(R)",
        "arg_schema": {
            "column_names": [
                "pageRank","pageURL","avgDuration"
            ],
            "column_types": [
                "LONG_TYPE","STRING_TYPE","LONG_TYPE"
            ]
        },
        "op_name": "Gather(R)",
        "op_type": "ShuffleConsumer"
    }

    getherS = {
        "arg_operator_id": "Shuffle(S)",
        "arg_schema": {
            "column_names": [
                "sourceIPAddr","destinationURL","visitDate","adRevenue","UserAgent","cCode","lCode","sKeyword", "avgTimeOnSite"
            ],
            "column_types": [
                "STRING_TYPE","STRING_TYPE","DATETIME_TYPE","FLOAT_TYPE","STRING_TYPE","STRING_TYPE","STRING_TYPE","STRING_TYPE","LONG_TYPE"
            ]
        },
        "op_name": "Gather(S)",
        "op_type": "ShuffleConsumer"
    }

    join = {
        "arg_child1": "Gather(R)",
            "arg_child2": "Gather(S)",
            "arg_columns1": [
                1
            ],
            "arg_columns2": [
                1
            ],
            "arg_select1": [
                0
            ],
            "arg_select2": [
                1
            ],
            "op_name": "Join",
            "op_type": "LocalJoin"
    }
    insert = {
        "arg_child": "Join",
        "op_name": "InsertResult",
        "op_type": "DbInsert",
        "relation_key": relation_key,
        "arg_overwrite_table": True,
        "connection_info": connection_info("localhost")
    }
    fragments = {
        "operators": [gatherR, getherS, join, insert]
    }

    return fragments

def generate_partition_join():
    
    table_R_key = {
        "program_name": "broadcastjoin",
        "relation_name": "RankBase2x",
        "user_name": "shumochu"
    }
    table_S_key = {
        "program_name": "broadcastjoin",
        "relation_name": "UserBase",
        "user_name": "shumochu"
    }
    result_key = {
        "program_name": "broadcastjoin",
        "relation_name": "PartitonJoinResult2x",
        "user_name": "shumochu"
    }

    fragments = [ scan_and_shuffle(table_R_key,"R"),
                  scan_and_shuffle(table_S_key,"S"),
                  receive_and_join(result_key)
                ]

    whole_plan = {
        "fragments": fragments,
        "logical_ra": "partiton join",
        "raw_datalog": "partition join"
    }
    return whole_plan

def scan_collect(relation_key):
    scan = {
        "op_name": "ScanR",
        "op_type": "TableScan",
        "relation_key": relation_key,
        "connection_info": connection_info("localhost")
    }
    collect_producer = {
        "arg_child": "ScanR",
        "arg_operator_id": "Collect",
        "op_name": "CollectProducer",
        "op_type": "CollectProducer"
    }
    fragment = {
        "operators": [scan, collect_producer]
    }
    return fragment

def collect_insert(relation_key, schema, worker_id):
    collect_consumer = {
        "arg_operator_id": "Collect",
        "arg_schema": schema,
        "op_name": "Gather",
        "op_type": "CollectConsumer"
    }
    insert = {
        "arg_child": "Gather",
        "op_name": "Insert",
        "op_type": "DbInsert",
        "relation_key": relation_key,
        "arg_overwrite_table": True,
        "connection_info": connection_info("localhost")
    }
    fragment = {
        "operators": [collect_consumer, insert],
        "workers": [ worker_id ]
    }
    return fragment

def concatenate():
    
    rank_schema = {
        "column_names": [ "pageRank","pageURL","avgDuration" ],
        "column_types": [ "LONG_TYPE","STRING_TYPE","LONG_TYPE" ]
    }
    user_schema = {
        "column_names": [
            "sourceIPAddr","destinationURL","visitDate","adRevenue","UserAgent","cCode","lCode","sKeyword", "avgTimeOnSite"
        ],
        "column_types": [
            "STRING_TYPE","STRING_TYPE","DATETIME_TYPE","FLOAT_TYPE","STRING_TYPE","STRING_TYPE","STRING_TYPE","STRING_TYPE","LONG_TYPE"
        ]
    }
    rank_table_key = {
        "program_name": "broadcastjoin",
        "relation_name": "RankBase2x",
        "user_name": "shumochu"
    }
    user_table_key = {
        "program_name": "broadcastjoin",
        "relation_name": "UserBase",
        "user_name": "shumochu"
    }
    rank_table_insert_key = {
        "program_name": "broadcastjoin",
        "relation_name": "RankBase2xUnion8workers",
        "user_name": "shumochu"
    }
    user_table_insert_key = {
        "program_name": "broadcastjoin",
        "relation_name": "UserBaseUnion8workers",
        "user_name": "shumochu"
    }

    fragments = [ scan_collect(rank_table_key), collect_insert(rank_table_insert_key, rank_schema, 1)]

    whole_plan = {
        "fragments": fragments,
        "logical_ra": "union table in 8 workers",
        "raw_datalog": "union table in 8 workers"
    }
    return whole_plan


#generate broadcast join plan
#print pretty_json(generate_broadcastjoin())

#generate partition join plan
#print pretty_json(generate_partition_join())

#concatenate the result table plan
print pretty_json(concatenate())
