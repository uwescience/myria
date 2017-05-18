#!/bin/sh

# Set up the database
psql -c 'create database myria_test;' -U postgres

# Set up SSH so that we can SSH to localhost
ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa_localhost -q
cat ~/.ssh/id_rsa_localhost.pub >> ~/.ssh/authorized_keys
ssh-keyscan -t rsa localhost >> ~/.ssh/known_hosts
echo "IdentityFile ~/id_rsa" >> ~/.ssh/config
echo "IdentityFile ~/id_rsa_localhost" >> ~/.ssh/config

# Set up myria-web
pip install paste
pip install webapp2
pip install webob
pip install jinja2
cd ~
git clone https://github.com/uwescience/myria-web.git
cd ~/myria-web
git submodule init
git submodule update
cd ~/myria-web/submodules/raco
git fetch --all && git reset --hard origin/blob_literal
python setup.py install

# Set up myria-python
pip install myria-python

# Install flake for style check
pip install flake8
