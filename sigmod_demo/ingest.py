#!/usr/bin/env python

import subprocess

host = 'localhost:8753'
INT = "INT_TYPE"
DOUBLE = "DOUBLE_TYPE"
STRING = "STRING_TYPE"

def do_ingest(table_name, schema):
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

do_ingest("edges", [('src', INT), ('dst', INT)])
do_ingest("vertices", [('id', INT)])
do_ingest('sc_points',[('id', INT), ('v', DOUBLE)])
do_ingest("all_opp_v3", [('Cruise', STRING), ("Day", STRING), ("File_Id", STRING), ("Cell_Id", INT),
  ("fsc_small", INT)])
do_ingest("all_vct", [('Cruise', STRING), ("Day", STRING), ("File_Id", STRING), ("Cell_Id", INT),
  ("pop", STRING)])
do_ingest("lat_lon_points", [('lat', DOUBLE), ('lon', DOUBLE)])
