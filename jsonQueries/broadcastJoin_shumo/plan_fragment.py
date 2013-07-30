#!/usr/bin/env python

import json

def pretty_json(obj):
    return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': '))

def jdbc_info(host):
    jdbc_info = {
        "driver_class": "com.vertica.jdbc.Driver",
        "dbms": "vertica",
        "host": host,
        "port": "15433",
        "username": "dbadmin",
        "database": "mrbenchmarks",
        "password": "mrbenchmarks"
    }

    return jdbc_info

def jdbc_count_and_collect(worker_id, host, table_name):
    jdbc_scan = {
        "op_name": "GetStats"+str(worker_id),
        "op_type": "JdbcQueryScan",
        "schema": {
            "column_types" : ["LONG_TYPE"],
            "column_names" : ["count"]
        },
        "sql": "SELECT count(*) from \""+table_name+ "\" ", 
        "jdbc_info":jdbc_info(host)                   
    }
    collect_producer = {
        "arg_child": "GetStats"+str(worker_id),
        "arg_operator_id": "Collect",
        "op_name": "CollectProducer"+str(worker_id),
        "op_type": "CollectProducer"
    }
    fragment = {
        "operators": [jdbc_scan, collect_producer],
        "workers": [ worker_id ]
    }

    return fragment

def collect_insert(worker_id, relation_key):
    collect_consumer = {
        "arg_operator_id": "Collect",
        "arg_schema": {
            "column_types" : ["LONG_TYPE"],
            "column_names" : ["count"]
        },
        "op_name": "CollectConsumer"+str(worker_id),
        "op_type": "CollectConsumer"
    }
    sqlite_insert = {
        "arg_child": "CollectConsumer"+str(worker_id),
        "arg_overwrite_table": True,
        "op_name": "Insert",
        "op_type": "SQLiteInsert",
        "relation_key": relation_key
    }
    fragment = {
        "operators": [ collect_consumer, sqlite_insert ],
        "workers": [worker_id]    
    }
    return fragment

def collect_result():
    fragments = []    
    data_table_name = "shumochu broadcastjoin result64x"
    relation_key = {
        "program_name": "broadcastJoin",
        "relation_name": "broadcastResult64x",
        "user_name": "shumochu"
    }
    for i in range(1,9):
        if i<8:
            host = "dbserver0"+str(i+2)+".cs.washington.edu"
        else: 
            host = "dbserver"+str(i+2)+".cs.washington.edu"
        fragments.append(jdbc_count_and_collect(i, host, data_table_name))
    fragments.append(collect_insert(1,relation_key))
    whole_plan = {
        "fragments": fragments,
        "logical_ra": "get result",
        "raw_datalog": "get result"
    }
    return whole_plan

def scan_broadcast(relation_key,worker_id,host):
    
    scan = {
        "op_name": "ScanR"+str(worker_id),
        "op_type": "JdbcScan",
        "relation_key": relation_key,
        "jdbc_info": jdbc_info(host)
    }
    
    broadcast = {
        "arg_child": "ScanR"+str(worker_id),
        "arg_operator_id": "broadcast",
        "op_name": "broadcast"+str(worker_id),
        "op_type": "BroadcastProducer"
    }

    fragment = {
        "operators": [ scan, broadcast ],
        "workers": [worker_id]
    }

    return fragment

def scan_join(relation_key,worker_id,host):
    scan = {
        "op_name": "ScanU"+str(worker_id),
        "op_type": "JdbcScan",
        "relation_key": {
            "program_name": "broadcastjoin",
            "relation_name": "UserBase",
            "user_name": "shumochu"
        },
        "jdbc_info":jdbc_info(host)
    }
    broadcast_consumer = {
        "arg_operator_id": "broadcast",
        "arg_schema": {
            "column_types" : ["LONG_TYPE","STRING_TYPE","LONG_TYPE"],
            "column_names" : ["pageRank","pageURL","avgDuration"]
        },
        "op_name": "receive"+str(worker_id),
        "op_type": "BroadcastConsumer"
    }
    join = {
        "arg_child1": "receive"+str(worker_id),
        "arg_child2": "ScanU"+str(worker_id),
        "arg_columns1": [ 1 ],
        "arg_columns2": [ 1 ],
        "arg_select1": [ 0 ],
        "arg_select2": [ 1 ],                
        "op_name": "Join"+str(worker_id),
        "op_type": "LocalJoin"
    }
    insert = {
        "arg_child": "Join"+str(worker_id),
        "op_name": "Insert"+str(worker_id),
        "op_type": "JdbcInsert",
        "relation_key": relation_key,
        "arg_overwrite_table": True,
        "jdbc_info": jdbc_info(host)
    }
    fragment = {
        "operators": [ scan, broadcast_consumer, join, insert ],
        "workers" : [ worker_id ]
    }

    return fragment

def generate_broadcastjoin():
    fragments = []
    table_R_key = {
        "program_name": "broadcastjoin",
        "relation_name": "RankBase64x",
        "user_name": "shumochu"
    }
    result_key = {
        "program_name": "broadcastjoin",
        "relation_name": "result64x",
        "user_name": "shumochu"
    }

    for i in range(1,9):
        if i<8:
            host = "dbserver0"+str(i+2)+".cs.washington.edu"
        else:
            host = "dbserver"+str(i+2)+".cs.washington.edu"
        fragments.append( scan_broadcast(table_R_key,i,host) )
        fragments.append( scan_join(result_key, i, host) )   

    whole_plan = {
        "fragments": fragments,
        "logical_ra": "broadcast join",
        "raw_datalog": "broadcast join"
    }
    return whole_plan

def scan_and_shuffle(relation_key, relation_name, worker_id, host):
    scan = {
        "op_name": "Scan("+relation_name+")@"+str(worker_id),
        "op_type": "JdbcScan",
        "relation_key": relation_key,
        "jdbc_info": jdbc_info(host)
    }
    shuffle = {
        "arg_child": "Scan("+relation_name+")@"+str(worker_id),
        "arg_operator_id": "Shuffle("+relation_name+")",
        "op_name": "Shuffle("+relation_name+")@"+str(worker_id),
        "op_type": "ShuffleProducer",
        "arg_pf": {
            "index": 1,
            "type": "SingleFieldHash"
        }
    }
    fragment = {
        "operators": [scan, shuffle ],
        "workers": [ worker_id ]
    }

    return fragment

def receive_and_join(relation_key,worker_id,host):
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
        "op_name": "Gather(R)@"+str(worker_id),
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
        "op_name": "Gather(S)@"+str(worker_id),
        "op_type": "ShuffleConsumer"
    }

    join = {
        "arg_child1": "Gather(R)@"+str(worker_id),
            "arg_child2": "Gather(S)@"+str(worker_id),
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
            "op_name": "Join"+str(worker_id),
            "op_type": "LocalJoin"
    }
    insert = {
        "arg_child": "Join"+str(worker_id),
        "op_name": "InsertResult@"+str(worker_id),
        "op_type": "JdbcInsert",
        "relation_key": relation_key,
        "arg_overwrite_table": True,
        "jdbc_info": jdbc_info(host)
    }
    fragments = {
        "operators": [gatherR, getherS, join, insert],
        "workers": [ worker_id ]
    }

    return fragments

def generate_partition_join():
    fragments = []
    table_R_key = {
        "program_name": "broadcastjoin",
        "relation_name": "RankBase64x",
        "user_name": "shumochu"
    }
    table_S_key = {
        "program_name": "broadcastjoin",
        "relation_name": "UserBase",
        "user_name": "shumochu"
    }
    result_key = {
        "program_name": "broadcastjoin",
        "relation_name": "PartitonJoinResult64x",
        "user_name": "shumochu"
    }

    for i in range(1,3):
        if i<8:
            host = "dbserver0"+str(i+2)+".cs.washington.edu"
        else:
            host = "dbserver"+str(i+2)+".cs.washington.edu"
        fragments.append( scan_and_shuffle(table_R_key,"R",i,host) )
        fragments.append( scan_and_shuffle(table_S_key,"S",i,host) )
        fragments.append( receive_and_join(result_key, i, host))

    whole_plan = {
        "fragments": fragments,
        "logical_ra": "partiton join",
        "raw_datalog": "partition join"
    }
    return whole_plan

def scan_collect(relation_key,worker_id,host):
    scan = {
        "op_name": "ScanR"+str(worker_id),
        "op_type": "JdbcScan",
        "relation_key": relation_key,
        "jdbc_info": jdbc_info(host)
    }
    collect_producer = {
        "arg_child": "ScanR"+str(worker_id),
        "arg_operator_id": "Collect",
        "op_name": "CollectProducer"+str(worker_id),
        "op_type": "CollectProducer"
    }
    fragment = {
        "operators": [scan, collect_producer],
        "workers": [ worker_id ]
    }
    return fragment

def collect_insert(relation_key, schema, worker_id, host):
    collect_consumer = {
        "arg_operator_id": "Collect",
        "arg_schema": schema,
        "op_name": "Gather",
        "op_type": "CollectConsumer"
    }
    insert = {
        "arg_child": "Gather",
        "op_name": "Insert"+str(worker_id),
        "op_type": "JdbcInsert",
        "relation_key": relation_key,
        "arg_overwrite_table": True,
        "jdbc_info": jdbc_info(host)
    }
    fragment = {
        "operators": [collect_consumer, insert],
        "workers": [ worker_id ]
    }
    return fragment

def concatenate():
    fragments = []
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
        "relation_name": "RankBase64x",
        "user_name": "shumochu"
    }
    user_table_key = {
        "program_name": "broadcastjoin",
        "relation_name": "UserBase",
        "user_name": "shumochu"
    }
    rank_table_insert_key = {
        "program_name": "broadcastjoin",
        "relation_name": "RankBase64xUnion8workers",
        "user_name": "shumochu"
    }
    user_table_insert_key = {
        "program_name": "broadcastjoin",
        "relation_name": "UserBaseUnion8workers",
        "user_name": "shumochu"
    }
    for i in range(1,9):
        if i<8:
            host = "dbserver0"+str(i+2)+".cs.washington.edu"
        else:
            host = "dbserver"+str(i+2)+".cs.washington.edu"
        fragments.append(scan_collect(rank_table_key, i, host))
        if i == 1:
            fragments.append(collect_insert(rank_table_insert_key, rank_schema, i, host))

    whole_plan = {
        "fragments": fragments,
        "logical_ra": "union table in 8 workers",
        "raw_datalog": "union table in 8 workers"
    }
    return whole_plan

def multiway_join_scan_shuffle():

print pretty_json(collect_result())
#print pretty_json(generate_broadcastjoin())
#print pretty_json(generate_partition_join())
#print pretty_json(concatenate())
