{
    "fragments": [
        {
            "operators": [
                {
                    "opId": 1,
                    "opType": "TableScan",
                    "relationKey": {
                        "programName": "broadcastjoin",
                        "relationName": "RankBase2x",
                        "userName": "shumochu"
                    }
                },
                {
                    "argChild": 1,
                    "distributeFunction": {
                        "indexes": [1],
                        "type": "Hash"
                    },
                    "opId": 3,
                    "opType": "ShuffleProducer"
                }
            ]
        },
        {
            "operators": [
                {
                    "opId": 2,
                    "opType": "TableScan",
                    "relationKey": {
                        "programName": "broadcastjoin",
                        "relationName": "UserBase",
                        "userName": "shumochu"
                    }
                },
                {
                    "argChild": 2,
                    "distributeFunction": {
                        "indexes": [1],
                        "type": "Hash"
                    },
                    "opId": 4,
                    "opType": "ShuffleProducer"
                }
            ]
        },
        {
            "operators": [
                {
                    "argOperatorId": 3,
                    "opId": 5,
                    "opType": "ShuffleConsumer"
                },
                {
                    "argOperatorId": 4,
                    "opId": 6,
                    "opType": "ShuffleConsumer"
                },
                {
                    "argChild1": 5,
                    "argChild2": 6,
                    "argColumns1": [
                        1
                    ],
                    "argColumns2": [
                        1
                    ],
                    "argSelect1": [
                        0
                    ],
                    "argSelect2": [
                        1
                    ],
                    "opId": 7,
                    "opType": "SymmetricHashJoin"
                },
                {
                    "argChild": 7,
                    "argOverwriteTable": true,
                    "opId": 8,
                    "opType": "DbInsert",
                    "relationKey": {
                        "programName": "broadcastjoin",
                        "relationName": "PartitonJoinResult2x",
                        "userName": "shumochu"
                    }
                }
            ]
        }
    ],
    "logicalRa": "partiton join",
    "rawQuery": "partition join"
}
