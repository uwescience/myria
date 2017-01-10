import sys
import argparse
import os
import stat
import json
import getpass

parser = argparse.ArgumentParser()
parser.add_argument("-a", "--all_filename", help="The deployment filename")
parser.add_argument("-d", "--deployment_filename", help="The deployment filename")
parser.add_argument("-s", "--setup_filename", help="The setup filename")
parser.add_argument("-i", "--ingest_filename", help="The ingest filename")
parser.add_argument("-r", "--run_filename", help="The run filename")
parser.add_argument("-t", "--teardown_filename", help="The teardown filename")
parser.add_argument("-hs", "--heap_size", type=int, help="The JVM heap size in GB, e.g. 1, 2, or 4. Default 2")
parser.add_argument("-p", "--rest_port_base", type=int, help="The base REST port number, will increment from this number")
parser.add_argument("-dn", "--data_nodes", type=int, help="The number of data nodes, expected [1:72]", required=True)
parser.add_argument("-sf", "--scale_factor", type=int, help="A scale factor to determine the number of compute nodes. The formula is #cn = scale_factor * #dn. Default 0, i.e. only data nodes")
parser.add_argument("-pp", "--postgresql_password", help="The PostgreSQL password", required=True)

args = parser.parse_args()

username = getpass.getuser()

num_dn=args.data_nodes
print '# of data nodes = %d' % num_dn

if args.scale_factor:
  num_cn = args.data_nodes * (args.scale_factor-1)
else: 
  num_cn = 0
print '# of compute nodes = %d' % num_cn
print 'Total # of nodes = %d' % (num_dn + num_cn)

if args.rest_port_base:
  port_base=args.rest_port_base
else:
  port_base=9023

if args.heap_size:
  heap_size=args.heap_size
else:
  heap_size=2

postgresql_password = args.postgresql_password

servers = [ 'aldebaran.cs.washington.edu', 'altair.cs.washington.edu', 'betelgeuse.cs.washington.edu', 'rigel.cs.washington.edu', 'spica.cs.washington.edu', 'castor.cs.washington.edu', 'pollux.cs.washington.edu', 'polaris.cs.washington.edu', 'deneb.cs.washington.edu', 'regulus.cs.washington.edu', 'capella.cs.washington.edu', 'algol.cs.washington.edu', 'mizar.cs.washington.edu', 'sirrah.cs.washington.edu', 'procyon.cs.washington.edu', 'alcor.cs.washington.edu', 'sol.cs.washington.edu', 'sirius.cs.washington.edu' ]



# write the deployment file
if args.deployment_filename:
  deployment_filename=args.deployment_filename
else:
  deployment_filename='deployment.cfg'
print 'writing the deployment file %s' % deployment_filename
deployment = open(deployment_filename, 'w')
deployment.write('# Deployment configuration\n')
deployment.write('[deployment]\n')
deployment.write('path = /disk1/myria\n')
deployment.write('name = testMyriadeploy\n')
deployment.write('dbms = postgresql\n')
deployment.write('database_password = %s\n' % postgresql_password) 
deployment.write('username = %s\n' % username)
deployment.write('max_heap_size = -Xmx%dg\n' % heap_size)
deployment.write('rest_port = 8999\n')
deployment.write('\n')
deployment.write('# Compute nodes configuration\n')
deployment.write('[master]\n')
deployment.write('0 = vega.cs.washington.edu:8091\n')
deployment.write('\n')
deployment.write('[workers]\n')
deployment.write('\n')
deployment.write('#list of data nodes\n')

port = port_base
if num_dn % len(servers) == 0:
  max_dn_per_server = num_dn / len(servers)
else:
  max_dn_per_server = num_dn / len(servers) + 1

if heap_size * max_dn_per_server > 48:
  print "cannot instantiate %d data nodes with heap size %d" % (max_dn_per_server, heap_size)
  print "total memory should be less than 48GB"
  sys.exit(1)

if max_dn_per_server > 4:
  print "cannot instantiate %d data nodes per server" % (max_dn_per_server)
  print "maximum allowed 4"
  sys.exit(1)

instance_port = port
n = 1
instance = 1
server = 0
while n <= num_dn:
  deployment.write('%d = %s:%d:/disk%d/myria:myria%d\n' % (n, servers[server], instance_port, instance, instance))
  n = n + 1
  server = server + 1
  if server == len(servers):
    server = 0
    instance = instance + 1
    instance_port = instance_port + 1
    deployment.write('\n')

deployment.write('\n')
deployment.write('#list of compute nodes\n')

if (num_dn+num_cn) % len(servers) == 0:
  max_wn_per_server = (num_dn+num_cn) / len(servers)
else:
  max_wn_per_server = (num_dn+num_cn) / len(servers) + 1

if heap_size * max_wn_per_server > 48:
  print "cannot instantiate %d worker nodes with heap size %d" % (max_wn_per_server, heap_size)
  print "total memory should be less than 48GB"
  sys.exit(1)

if max_wn_per_server > 24:
  print "cannot instantiate %d worker nodes per server" % (max_wn_per_server)
  print "maximum allowed 24"
  sys.exit(1)

instance = 1
while n <= (num_dn + num_cn):
  deployment.write('%d = %s:%d:/disk%d/myria:myria%d\n' % (n, servers[server], instance_port, instance, instance))
  n = n + 1
  server = server + 1
  if server == len(servers):
    server = 0
    instance_port = instance_port + 1
    deployment.write('\n')

deployment.close


# write the setup script
if args.setup_filename:
  setup_filename=args.setup_filename
  print 'Writing the setup script %s' % setup_filename

  setup = open(setup_filename, 'w')
  setup.write("#!/bin/sh\n")
  setup.write("\n")
  setup.write("./setup_cluster.py %s\n" % deployment_filename)
  setup.write("./start_master.py %s\n" % deployment_filename)
  setup.write("./start_workers.py %s\n" % deployment_filename)
  setup.write("\n")
  setup.write("curl -i vega:8999/workers\n")
  setup.write("curl -i vega:8999/workers/alive | python count.py\n")
  setup.write("\n")
  setup.write("export PGPASSWORD=%s\n" % postgresql_password)
  setup.write("\n")

  n = 1
  instance = 1
  server = 0
  while n <= num_dn:
    setup.write("psql -h %s -p 5401 -U uwdb -c \"drop table \\\"%s twitter_join twitter_%ddn\\\";\" myria%d\n" % (servers[server], username, num_dn, instance))
    n = n + 1
    server = server + 1
    if server == len(servers):
      server = 0
      instance = instance + 1
      deployment.write('\n')

  setup.close
  st = os.stat(setup_filename)
  os.chmod(setup_filename, st.st_mode | stat.S_IEXEC)


# write the ingest script
if args.ingest_filename:
  ingest_filename=args.ingest_filename
  print 'Writing the ingest script %s' % ingest_filename

  ingest = open(ingest_filename, 'w')
  ingest.write("#!/bin/sh\n")
  ingest.write("\n")
  ingest.write("export PGPASSWORD=%s\n" % postgresql_password)
  ingest.write("\n")
  ingest.write("curl -i -XPOST vega:8999/dataset -H \"Content-type: application/json\"  -d @./ingest_twitter_2_workers.json\n")
  ingest.close
  st = os.stat(ingest_filename)
  os.chmod(ingest_filename, st.st_mode | stat.S_IEXEC)

# write the run script
if args.run_filename:
  run_filename=args.run_filename
  print 'Writing the run script %s' % run_filename

  run = open(run_filename, 'w')
  run.write("#!/bin/sh\n")
  run.write("\n")
  run.write("export PGPASSWORD=%s\n" % postgresql_password)
  run.write("\n")
  n = num_dn
  while n <= (num_dn + num_cn):
    for i in range(1,5):
      run.write("curl -i -XPOST vega:8999/query -H \"Content-type: application/json\" -d @./myfiles/query_twitter_join_%d_%d.json\n" % (num_dn, n))
      run.write("sleep 10\n")
      run.write("tail -n 1 /disk1/myria/testMyriadeploy-files/master_stdout\n")
      run.write("\n")
    n = n * 2

  run.close
  st = os.stat(run_filename)
  os.chmod(run_filename, st.st_mode | stat.S_IEXEC)

  n = num_dn
  while n <= (num_dn+num_cn):

    query_filename = "myfiles/query_twitter_join_%d_%d.json" % (num_dn, n)
    query = open(query_filename, 'w')

    relation_key = {
      "programName": "twitter_join",
      "relationName": "twitter_%ddn" % num_dn,
      "userName": username 
    }

    scan1 = {
      "opId": "SCAN1",
      "opType": "TableScan",
      "relationKey": relation_key
    }

    shuffle1 = {
      "argChild": "SCAN1",
      "argOperatorId": "0",
      "opId": "SP1",
      "opType": "ShuffleProducer",
      "distributeFunction": {
        "indexes": [1],
        "type": "Hash"
      }
    }

    data_nodes = range(1,num_dn+1)
    fragment1 = {
      "operators": [scan1, shuffle1 ],
      "workers": data_nodes
    }

    scan2 = {
      "opId": "SCAN2",
      "opType": "TableScan",
      "relationKey": relation_key
    }

    shuffle2 = {
      "argChild": "SCAN2",
      "argOperatorId": "1",
      "opId": "SP2",
      "opType": "ShuffleProducer",
      "distributeFunction": {
        "indexes": [1],
        "type": "Hash"
      }
    }
 
    fragment2 = {
      "operators": [scan2, shuffle2 ],
      "workers": data_nodes
    }
 
    shuffle_consumer1 = {
      "argOperatorId": "0",
      "arg_schema": {
        "columnTypes" : ["LONG_TYPE","LONG_TYPE"],
        "columnNames" : ["follower","followee"]
      },
      "opId": "SC1",
      "opType": "ShuffleConsumer"
    }

    shuffle_consumer2 = {
      "argOperatorId": "1",
      "arg_schema": {
        "columnTypes" : ["LONG_TYPE","LONG_TYPE"],
        "columnNames" : ["follower","followee"]
      },
      "opId": "SC2",
      "opType": "ShuffleConsumer"
    }
 
    join = {
      "argChild1": "SC1",
      "argChild2": "SC2",
      "argColumns1": [
        "1"
      ],
      "argColumns2": [
        "0"
      ],
      "argSelect1": [
        "0"
      ],
      "argSelect2": [
        "1"
      ],
      "opId": "JOIN",
      "opType": "SymmetricHashJoin"
    }

    sink_root = {
      "argChild": "JOIN",
      "opId": "SINK",
      "opType": "SinkRoot"
    }

    worker_nodes = range(1, n+1)
    fragment3 = {
      "operators": [ shuffle_consumer1, shuffle_consumer2, join, sink_root ],
      "workers": worker_nodes
    }
 
    fragments = [ fragment1, fragment2, fragment3 ]

    plan = {
      "fragments": fragments,
      "logicalRa": "SINK(JOIN(SCAN1,SCAN2))",
      "rawQuery": "twitter(x,z) :- twitter(x,y),twitter(y,z)."
    }

    query.write(json.dumps(plan, indent=2))
    query.close

    n = n * 2

# write the teardown script
if args.teardown_filename:
  teardown_filename=args.teardown_filename
  print 'Writing the teardown script %s' % teardown_filename

  teardown = open(teardown_filename, 'w')
  teardown.write("#!/bin/sh\n")
  teardown.write("\n")
  teardown.write("curl -i vega:8999/server/shutdown\n")

  n = 0
  while n < (num_dn+num_cn) and n < len(servers):
    teardown.write("ssh -l %s %s \"pkill java\"\n" % (username, servers[n]))
    n = n + 1

  teardown.close
  st = os.stat(teardown_filename)
  os.chmod(teardown_filename, st.st_mode | stat.S_IEXEC)

# write the script to run all scripts
if args.all_filename:
  all_filename=args.all_filename
  print 'writing the all script %s' % all_filename

  allsh = open(all_filename, 'w')
  allsh.write("#!/bin/sh\n")
  allsh.write("\n")
  allsh.write('%s\n' % setup_filename);
  allsh.write('%s\n' % ingest_filename);
  allsh.write('%s\n' % run_filename);
  allsh.write('cp /disk1/myria/testMyriadeploy-files/master_stdout myfiles/master_%ddn_stdout\n' % num_dn);
  allsh.write('%s\n' % teardown_filename);
  allsh.close
  st = os.stat(all_filename)
  os.chmod(all_filename, st.st_mode | stat.S_IEXEC)

