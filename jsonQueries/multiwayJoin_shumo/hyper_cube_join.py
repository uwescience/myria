#!/usr/bin/env python

import json

def pretty_json(obj):
    return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ':'))

def coordinate_to_worker_id(coordinate, dimSizes):
    result = 0
    for k,v in enumerate(coordinate):
        result = result+v
        if k != (len(dimSizes)-1):
            result =  result * dimSizes[k]
    return result
        
def get_coordinates(coordinates,dimSizes, coordinate , currentIndex): 
    if(currentIndex == len(dimSizes)):
        coordinates.append(coordinate)
    else:
        for i in range(0,dimSizes[currentIndex]):
            cp_coordinates = coordinate[:]
            cp_coordinates.append(i)         
            get_coordinates(coordinates, dimSizes, cp_coordinates , currentIndex+1)
            
def hyper_cube_partition(dimSizes,hashedDims):
    coordinates = []
    get_coordinates(coordinates, dimSizes, [], 0)
    cell_partition = dict()
    for coordinate in coordinates:
        cell_number = 0
        count = 1
        for dim in hashedDims:
            cell_number += coordinate[dim]
            if count != len(dimSizes):
                cell_number *= dimSizes[dim]
            count += 1
        if cell_partition.has_key(cell_number):
            cell_partition[cell_number].append( coordinate_to_worker_id(coordinate,dimSizes) )
        else:
            cell_partition[cell_number] = [coordinate_to_worker_id(coordinate,dimSizes)]          

    return  [ v[1] for v in sorted(cell_partition.items()) ]      

#change relational key here
def twitter_relation_key():
    relation_key = {
        "user_name" : "chushumo",
        "program_name" : "multiway_join",
        "relation_name" : "twitter_small"
    }
    return relation_key

#change schema here
def twitter_schema():
    schema = {
        "column_types" : ["INT_TYPE", "INT_TYPE"],
        "column_names" : ["follower", "followee"]
    }
    return schema

#Change hypercube dimisions here
def hyperCubeDims():
    return [3,3,3]

def shuffle_r():
    scan = {
        "op_type" : "DbQueryScan",
        "op_name" : "Scan(R)",
        "sql": "select * from \"chushumo#multiway_join#twitter_full\" as a where a.follower<a.followee",
        "schema" : twitter_schema()
    }
    shuffle = {
        "op_type" : "HyperShuffleProducer",
        "op_name" : "shuffle_r",
        "arg_child" : "Scan(R)",
        "field_indexes" : [0,1],
        "cell_partition":hyper_cube_partition(hyperCubeDims(),[0,1]),
        "hyper_cube_dimensions": hyperCubeDims()   
    }
    fragment = {
        "operators": [scan, shuffle]
    }
    return fragment

def shuffle_s():
    scan = {
        "op_type" : "DbQueryScan",
        "op_name" : "Scan(S)",
        "sql": "select * from \"chushumo#multiway_join#twitter_full\"",
        "schema" : twitter_schema()
    }
    shuffle = {
        "op_type" : "HyperShuffleProducer",
        "op_name" : "shuffle_s",
        "arg_child" : "Scan(S)",
        "field_indexes" : [0,1],
        "cell_partition":hyper_cube_partition(hyperCubeDims(),[1,2]),
        "hyper_cube_dimensions":hyperCubeDims()     
    }
    fragment = {
        "operators": [scan, shuffle]
    }
    return fragment

def shuffle_t():
    scan = {
        "op_type" : "DbQueryScan",
        "op_name" : "Scan(T)",
        "sql": "select * from \"chushumo#multiway_join#twitter_full\" as a where a.follower>a.followee",
        "schema" : twitter_schema()
    }
    shuffle = {
        "op_type" : "HyperShuffleProducer",
        "op_name" : "shuffle_t",
        "arg_child" : "Scan(T)",
        "field_indexes" : [1,0],
        "cell_partition": hyper_cube_partition(hyperCubeDims(),[0,2]),
        "hyper_cube_dimensions":hyperCubeDims()     
    }
    fragment = {
        "operators": [scan, shuffle]
    }
    return fragment

def receive_then_split():
    gatherT = {
        "op_type" : "HyperShuffleConsumer",
        "op_name" : "gatherT",
        "arg_operator_id" : "shuffle_t",
        "arg_schema" : {
            "column_types" : ["INT_TYPE", "INT_TYPE"],
            "column_names" : ["z", "x"]
        }
    }
    split_T = {
        "arg_child": "gatherT",
        "arg_operator_ids": [
            "t2",
            "t3",
            "t"
        ],
        "op_name": "split_T",
        "op_type": "LocalMultiwayProducer"
    }
    fragment = {
        "operators": [gatherT, split_T]
    }
    return fragment

def semi_join_then_join():
    gatherR = {
        "op_type" : "HyperShuffleConsumer",
        "op_name" : "gatherR",
        "arg_operator_id" : "shuffle_r",
        "arg_schema" : {
            "column_types" : ["INT_TYPE", "INT_TYPE"],
            "column_names" : ["x", "y"]
        }
    }
    gatherS = {
        "op_type" : "HyperShuffleConsumer",
        "op_name" : "gatherS",
        "arg_operator_id" : "shuffle_s",
        "arg_schema" : {
            "column_types" : ["INT_TYPE", "INT_TYPE"],
            "column_names" : ["y", "z"]
        }
    }
    receive_T = {
        "arg_operator_id": "t",
        "arg_schema": {
            "column_names": [
                "z",
                "x"
            ],
            "column_types": [
                "INT_TYPE",
                "INT_TYPE"
            ]
        },
        "op_name": "receive_T",
        "op_type": "LocalMultiwayConsumer"
    }
    receive_T2 = {
        "arg_operator_id": "t2",
        "arg_schema": {
            "column_names": [
                "z",
                "x"
            ],
            "column_types": [
                "INT_TYPE",
                "INT_TYPE"
            ]
        },
        "op_name": "receive_T2",
        "op_type": "LocalMultiwayConsumer"
    }
    receive_T3 = {
        "arg_operator_id": "t3",
        "arg_schema": {
            "column_names": [
                "z",
                "x"
            ],
            "column_types": [
                "INT_TYPE",
                "INT_TYPE"
            ]
        },
        "op_name": "receive_T3",
        "op_type": "LocalMultiwayConsumer"
    }
    project_T_on_x = {
        "arg_field_list": [1],
        "arg_child" : "receive_T2",
        "op_name": "project_T_on_x",
        "op_type": "Project"
    }
    T2 = {
        "arg_child" : "project_T_on_x",
        "op_name" : "T2",
        "op_type" : "DupElim"
    }
    project_T_on_z = {
        "arg_field_list": [0],
        "arg_child" : "receive_T3",
        "op_name": "project_T_on_z",
        "op_type": "Project"
    }
    T3 = {
        "arg_child" : "project_T_on_z",
        "op_name" : "T3",
        "op_type" : "DupElim"
    }
    semi_join_T2_R1 = {
        "op_type" : "LocalUnbalancedJoin",
        "op_name" : "semi_join_T2_R1",
        "arg_child1" : "T2",
        "arg_child2" : "gatherR",
        "arg_columns1" : [0],
        "arg_columns2" : [0],
        "arg_select1" : [0],
        "arg_select2" : [1]
    }
    semi_join_T3_S1 = {
        "op_type" : "LocalUnbalancedJoin",
        "op_name" : "semi_join_T3_S1",
        "arg_child1" : "gatherS",
        "arg_child2" : "T3",
        "arg_columns1" : [1],
        "arg_columns2" : [0],
        "arg_select1" : [0],
        "arg_select2" : [0]
    }
    join_R2_S2 = {
        "op_type" : "LocalUnbalancedJoin",
        "op_name" : "join_R2_S2",
        "arg_child1" : "semi_join_T2_R1",
        "arg_child2" : "semi_join_T3_S1",
        "arg_columns1" : [1],
        "arg_columns2" : [0],
        "arg_select1" : [0,1],
        "arg_select2" : [1]
    }
    join_R_S_T = {
        "op_type" : "LocalUnbalancedJoin",
        "op_name" : "Join(R,S,T)",
        "arg_child1" : "join_R2_S2",
        "arg_child2" : "receive_T",
        "arg_columns1" : [0,2],
        "arg_columns2" : [1,0],
        "arg_select1" : [0,1],
        "arg_select2" : [0]
    }
    join_R_S_T_count = {
        "op_type" : "LocalUnbalancedCountingJoin",
        "op_name" : "Join(R,S,T)",
        "arg_child1" : "join_R2_S2",
        "arg_child2" : "receive_T",
        "arg_columns1" : [0,2],
        "arg_columns2" : [1,0]
    }
    insert = {
        "arg_child": "Join(R,S,T)",
        "arg_overwrite_table": True,
        "op_name": "Insert",
        "op_type": "DbInsert",
        "relation_key": {
            "program_name": "multiway_join",
            "relation_name": "twitter_triangle_hyperjoin",
            "user_name": "chushumo"
        }
    }
    insertCount = {
        "arg_child": "Join(R,S)",
        "arg_overwrite_table": True,
        "op_name": "Insert",
        "op_type": "DbInsert",
        "relation_key": {
            "program_name": "multiway_join",
            "relation_name": "twitter_triangle_hyperjoin_count",
            "user_name": "chushumo"
        }
    }
    collect = {
        "arg_child": "Join(R,S,T)",
        "arg_operator_id": "collect",
        "op_name": "SendResult",
        "op_type": "CollectProducer"
    }
    fragment = {
        "operators": [ gatherR, gatherS, receive_T, receive_T2, receive_T3, 
                        project_T_on_x, T2, project_T_on_z, T3, 
                        semi_join_T2_R1, semi_join_T3_S1, join_R2_S2, join_R_S_T_count, insert]
        # only counting tuples
        #"operators": [gatherR, gatherS, count_join_R_S, insertCount]
    }    
    return fragment

def gather_result():
    receive = {
        "op_type" : "ShuffleConsumer",
        "op_name" : "Gather",
        "arg_operator_id" : "collect",
        "arg_schema" : {
            "column_names":[ "x", "y", "z" ],
            "column_types":[ "INT_TYPE", "INT_TYPE","INT_TYPE" ]
        }
    }
    insert = {
        "arg_child": "Gather",
        "arg_overwrite_table": True,
        "op_name": "Insert",
        "op_type": "DbInsert",
        "relation_key": {
            "user_name" : "chushumo",
            "program_name" : "multiway_join",
            "relation_name" : "twitter_small_triangle_combined"
        }
    }
    fragment = {
        "operators": [ receive, insert ],
        "workers" : [ 1 ]
    }
    return fragment

def twitter_triangle():
    fragments = [shuffle_r(), shuffle_s(), shuffle_t(), receive_then_split(), semi_join_then_join()]
    #only counting tuples
    #fragments = [shuffle_r(), shuffle_s(), receive_then_join()]

    whole_plan = {
        "fragments":fragments,
        "logical_ra" : "hyper cube join ",
        "raw_datalog" : "hyper cube join",
        "profiling_mode": True
    }
    return whole_plan

def receive_t_and_insert():
    gatherT = {
        "op_type" : "HyperShuffleConsumer",
        "op_name" : "gatherT",
        "arg_operator_id" : "shuffle_t",
        "arg_schema" : {
            "column_types" : ["INT_TYPE", "INT_TYPE"],
            "column_names" : ["z", "x"]
        }
    }
    insertT = {
                "arg_child": "gatherT",
                "arg_overwrite_table": True,
                "op_name": "InsertT",
                "op_type": "DbInsert",
                "relation_key": {
                    "user_name" : "chushumo",
                    "program_name" : "multiway_join",
                    "relation_name" : "t"
                }
            }
    fragment = {
        "operators": [ gatherT, insertT ]
    }
    return fragment

def test_t_size():
    fragments = [shuffle_t(),receive_t_and_insert()]

    whole_plan = {
        "fragments":fragments,
        "logical_ra" : "check t",
        "raw_datalog" : "check t",
        "profiling_mode": True
    }
    return whole_plan

def scan_and_collect():
    scan = {
        "op_type" : "DbQueryScan",
        "op_name" : "Scan(T)",
        "schema" : {
            "column_names":[ "number"],
            "column_types":[ "LONG_TYPE"]
        },
        "sql": "SELECT count(*) FROM \"chushumo"+"#"+"multiway_join"+"#"+"t\""
    }
    collect = {
        "arg_child": "Scan(T)",
        "arg_operator_id": "collect",
        "op_name": "SendResult",
        "op_type": "CollectProducer"
    }
    fragment = {
        "operators": [ scan, collect ]
    }
    return fragment

def receive_result():
    gatherT = {
        "op_type" : "CollectConsumer",
        "op_name" : "gatherT",
        "arg_operator_id" : "collect",
        "arg_schema" : {
                "column_names":[ "number"],
                "column_types":[ "LONG_TYPE"]
            }
    }
    insertT = {
                "arg_child": "gatherT",
                "arg_overwrite_table": True,
                "op_name": "InsertT",
                "op_type": "DbInsert",
                "relation_key": {
                    "user_name" : "chushumo",
                    "program_name" : "multiway_join",
                    "relation_name" : "t_count"
                }
            }
    fragment = {
        "operators": [ gatherT, insertT ],
        "workers":[1]
    }
    return fragment

def get_t_stats():
    fragments = [scan_and_collect(),receive_result()]
    
    whole_plan = {
        "fragments":fragments,
        "logical_ra" : "get t stats",
        "raw_datalog" : "get t stats",
        "profiling_mode": True
    }
    return whole_plan

def receive_and_insert(relation):
    relation_name = "gather%s"%str(relation);
    gatherR = {
        "op_type" : "HyperShuffleConsumer",
        "op_name" : relation_name,
        "arg_operator_id" : "shuffle_%s"%str(relation).lower()
    }
    recordR = {
        "arg_child": relation_name,
        "arg_overwrite_table": True,
        "op_name": "Insert%s"%str(relation),
        "op_type": "DbInsert",
        "relation_key": {
            "user_name" : "chushumo",
            "program_name" : "multiway_join",
            "relation_name" : "%s1"%str(relation).lower()
        }
    }
    fragment = {
        "operators": [gatherR, recordR]
    }
    return fragment

def shuffle_r_plan():
    fragments = [shuffle_r(),receive_and_insert("R")]
    whole_plan = {
        "fragments":fragments,
        "logical_ra" : "shuffle_r",
        "raw_datalog" : "shuffle_r",
        "profiling_mode": True
    }
    return whole_plan

def shuffle_s_plan():
    fragments = [shuffle_s(),receive_and_insert("S")]
    whole_plan = {
        "fragments":fragments,
        "logical_ra" : "shuffle_s",
        "raw_datalog" : "shuffle_s",
        "profiling_mode": True
    }
    return whole_plan

def shuffle_t_plan():
    fragments = [shuffle_t(),receive_and_insert("T")]
    whole_plan = {
        "fragments":fragments,
        "logical_ra" : "shuffle_t",
        "raw_datalog" : "shuffle_t",
        "profiling_mode": True
    }
    return whole_plan

def scan_then_split():
    scan_T = {
        "op_type" : "TableScan",
        "op_name" : "Scan_T1",
        "relation_key": {
            "user_name" : "chushumo",
            "program_name" : "multiway_join",
            "relation_name" : "t1"
        }
    }
    split_T = {
        "arg_child": "gatherT",
        "op_name": "split_t",
        "op_type": "LocalMultiwayProducer"
    }
    fragment = {
        "operators": [scan_T, split_T]
    }
    return fragment

def semi_join_T1_R1():
    scan_R = {
        "op_type" : "TableScan",
        "op_name" : "Scan_R1",
        "relation_key": {
            "user_name" : "chushumo",
            "program_name" : "multiway_join",
            "relation_name" : "r1"
        }
    }
    scan_T = {
        "op_type" : "TableScan",
        "op_name" : "Scan_T1",
        "relation_key": {
            "user_name" : "chushumo",
            "program_name" : "multiway_join",
            "relation_name" : "t1"
        }
    }
    project_T_on_x = {
        "arg_field_list": [1],
        "arg_child" : "Scan_T1",
        "schema": {
            "column_types" : ["INT_TYPE"],
            "column_names" : ["t_follower"]
        },
        "op_name": "project_T_on_x",
        "op_type": "ColumnSelect"
    }
    T2 = {
        "arg_child" : "project_T_on_x",
        "op_name" : "T2",
        "op_type" : "DupElim"
    }
    semi_join_T2_R1 = {
        "op_type" : "RightHashJoin",
        "op_name" : "semi_join_T2_R1",
        "arg_child1" : "T2",
        "arg_child2" : "Scan_R1",
        "arg_columns1" : [0],
        "arg_columns2" : [0],
        "arg_select1" : [0],
        "arg_select2" : [1]
    }
    recordR = {
        "arg_child": "semi_join_T2_R1",
        "arg_overwrite_table": True,
        "op_name": "insert_t2_r1",
        "op_type": "DbInsert",
        "relation_key": {
            "user_name" : "chushumo",
            "program_name" : "multiway_join",
            "relation_name" : "t2_r1"
        }
    }
    fragment = {
        "operators": [scan_R,scan_T,project_T_on_x,T2,semi_join_T2_R1, recordR]
    }    
    return fragment

def semi_join_T1_R2_plan():
    fragments = [semi_join_T1_R1()]
    whole_plan = {
        "fragments":fragments,
        "logical_ra" : "semi_join_T1_R1",
        "raw_datalog" : "semi_join_T1_R1",
        "profiling_mode": True
    }
    return whole_plan

#output json
#print pretty_json(twitter_triangle())
#print pretty_json(test_t_size())
#print pretty_json(get_t_stats())
#print pretty_json(shuffle_r_plan())
#print pretty_json(shuffle_s_plan())
#print pretty_json(shuffle_t_plan())
print pretty_json(semi_join_T1_R2_plan())