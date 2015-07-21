#!/usr/bin/env python

"""Generates per-worker scripts to alter all Postgres table columns of FLOAT_TYPE from DOUBLE PRECISION to REAL. Should be run on master node."""

import sys
from collections import defaultdict
import sqlalchemy
import sqlalchemy.orm

catalog_file = sys.argv[1] if len(sys.argv) > 1 else "master.catalog"

# The sqlalchemy connection to the actual database
db_url = "sqlite:///%s" % catalog_file
db = sqlalchemy.create_engine(db_url)

meta = sqlalchemy.MetaData(bind=db)
meta.reflect()

session = sqlalchemy.orm.sessionmaker(bind=db)()

Relations = meta.tables['relations']
StoredRelations = meta.tables['stored_relations']
Shards = meta.tables['shards']
RelationSchema = meta.tables['relation_schema']
Workers = meta.tables['workers']

statement = RelationSchema.join(Relations).join(StoredRelations).join(Shards).join(Workers).select().with_only_columns(
[StoredRelations.columns.user_name,
 StoredRelations.columns.program_name,
 StoredRelations.columns.relation_name,
 RelationSchema.columns.col_name,
 Workers.columns.host_port
]).where(RelationSchema.columns.col_type == 'FLOAT_TYPE')

columns_by_worker = defaultdict(list)
result = session.execute(statement).fetchall()
for row in result:
    user_name, program_name, relation_name, col_name, host_port = row
    columns_by_worker[host_port].append(("%s:%s:%s" % (user_name, program_name, relation_name), col_name))

program_template = """
import psycopg2

with psycopg2.connect(
    host='%s',
    port=%d,
    database='myria-production',
    user='myria-production',
    password='PaulAllenCenter'
) as conn:
    with conn.cursor() as cur:
        cur.execute('''%s''')
"""

for host_port, table_columns in columns_by_worker.iteritems():
    host, port = host_port.split(":")
    statements = []
    for table, column in table_columns:
        statement = 'ALTER TABLE "%s" ALTER COLUMN "%s" SET DATA TYPE REAL;' % (table, column)
        statements.append(statement)
    sql = "\n".join(statements)
    print program_template % (host, int(port), sql)
