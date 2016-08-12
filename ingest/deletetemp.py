import ConfigParser, os
import subprocess

deployment_file = open('/usr/local/myria/myriadeploy/deployment.cfg')

config = ConfigParser.ConfigParser()
config.readfp(deployment_file)

drop_cmd = """
ssh {w} "sudo -u postgres psql {m} -c \\"select 'drop table ' || '\\\\\\\"' || tablename|| '\\\\\\\";' from pg_tables where tablename like '%:temp:%'\\" -t | sudo -u postgres psql {m}"
"""
get_tables_cmd = """
ssh {w} "sudo -u postgres psql {m} -c \\"select 'drop table ' || '\\\\\\\"' || tablename|| '\\\\\\\";' from pg_tables where tablename like '%:temp:%'\\" -t"
"""
restart_postgres = """
ssh {w} "sudo /etc/init.d/postgresql restart"
"""

for worker in config.options('workers'):
    w = config.get('workers',worker)
    worker_host = w.split(':')[0]
    db_name = w.split(':')[3]
    p = subprocess.Popen(get_tables_cmd.format(w=worker_host, m=db_name), shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output = "".join(p.stdout.readlines()).strip()
    if output != '':
        #os.system(restart_postgres.format(w=worker_host))
        p = subprocess.Popen(drop_cmd.format(w=worker_host, m=db_name), shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
	print "".join(p.stdout.readlines()).strip()
        
