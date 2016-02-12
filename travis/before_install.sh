#!/bin/sh

# Set up the database
psql -c 'create database myria_test;' -U postgres

# Set up SSH so that we can SSH to localhost
ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa_localhost -q
cat ~/.ssh/id_rsa_localhost.pub >> ~/.ssh/authorized_keys
ssh-keyscan -t rsa localhost >> ~/.ssh/known_hosts
echo "IdentityFile ~/id_rsa" >> ~/.ssh/config
echo "IdentityFile ~/id_rsa_localhost" >> ~/.ssh/config

python -c 'import boto, boto.s3.key, time, os; print repr(os.environ); b = boto.connect_s3(os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"]).get_bucket("myria-test"); boto.s3.key.Key(bucket=b, name="myria-write-test-%d" % int(time.time())).set_contents_from_string("Myria S3 write test")'
