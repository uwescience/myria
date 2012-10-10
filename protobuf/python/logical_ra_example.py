#!/usr/bin/env python

from logical_ra_pb2 import *
import google
from google.protobuf import text_format
import sys

def toPrintableString(in_string):
    ret = ""
    for byte in in_string:
        if byte in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890!@#$%^&*()-=_+`~[]{};':\",./<>? ":
            ret += byte
        else:
            ret += '.'
    return ret

# List of operators used to generate the final message
operators = []

# Start by constructing the 3 scans
for i in range(1,4):
    scan = LogicalRaOperator()
    scan.type = LogicalRaOperator.SCAN
    scan.name = "Scan"+str(i)
    scan.scan.relation = "R"
    operators.append(scan)

# Construct the first Join
join1 = LogicalRaOperator()
join1.type = LogicalRaOperator.JOIN
join1.name="Join1"
join1.join.leftChildName = "Scan1"
join1.join.leftColumn.extend([2])
join1.join.rightChildName = "Scan2"
join1.join.rightColumn.extend([0])
operators.append(join1)

# Construct the second Join
join2 = LogicalRaOperator()
join2.type = LogicalRaOperator.JOIN
join2.name="Join2"
join2.join.leftChildName = "Join1"
join2.join.leftColumn.extend([6])
join2.join.rightChildName = "Scan3"
join2.join.rightColumn.extend([0])
operators.append(join2)

query = LogicalRaQueryMessage()
query.name = "SampleQuery"
query.operator.extend(operators)

try:
    serialized = query.SerializeToString()
except google.protobuf.message.EncodeError as e:
    print "Error caught serializing query: e"
    sys.exit(1)

print "\t%d bytes: %s" %(len(serialized),toPrintableString(serialized))
print

# For debug purposes, print this message as a user-readable string
print text_format.MessageToString(query)
print
