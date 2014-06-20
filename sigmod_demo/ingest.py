#!/usr/bin/env python

import subprocess

host = 'localhost:8753'
INT = "INT_TYPE"
DOUBLE = "DOUBLE_TYPE"
STRING = "STRING_TYPE"

def do_create(table_name, schema):
    names, types = zip(*schema)
    names_str = ','.join(['"%s"' % s for s in names])
    types_str = ','.join(['"%s"' % s for s in types])

    json = """{
      "relationKey" : {
        "userName" : "public",
        "programName" : "adhoc",
        "relationName" : "%s"
      },
      "schema" : {
        "columnNames" : [%s],
        "columnTypes" : [%s]
      },
      "source" : {
        "dataType" : "Empty"
      }
    }""" % (table_name, names_str, types_str)
    print json

    subprocess.call(['curl', '-i', '-XPOST', '%s/dataset' % host, '-H',
      'Content-type: application/json',  '-d',  json])

def do_ingest(table_name, phile):
    with open(phile) as fh:
        contents = fh.read()
    url = '%s/dataset/user-public/program-adhoc/relation-%s/data?format=csv' % (
        host, table_name)
    args = ['curl', '-i', '-XPUT', url,
            '-H', 'Content-type: application/octet-stream',
            '--data-binary', contents]
    print ' '.join(args)
    subprocess.call(args)

do_create("edges", [('src', INT), ('dst', INT)])
do_create("vertices", [('id', INT)])
do_create('sc_points',[('id', INT), ('v', DOUBLE)])
do_create("all_opp_v3", [('Cruise', STRING), ("Day", STRING), ("File_Id", STRING), ("Cell_Id", INT),
  ("fsc_small", INT)])
do_create("all_vct", [('Cruise', STRING), ("Day", STRING), ("File_Id", STRING), ("Cell_Id", INT),
  ("pop", STRING)])
do_create("lat_lon_points", [('lat', DOUBLE), ('lon', DOUBLE)])
do_create("employee", [('id', 'INT_TYPE'), ('dept_id', 'INT_TYPE'), ('name', 'STRING_TYPE'),
                       ('salary', "INT_TYPE")])
do_create("department", [('id', 'INT_TYPE'),  ('name', 'STRING_TYPE'),
                         ('manager', 'INT_TYPE')])

do_ingest("employee", 'emp.csv')
do_ingest("department", 'dept.csv')
