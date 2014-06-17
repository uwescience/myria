#!/usr/bin/env python

import os
import subprocess
from time import sleep

dd = '../myriadeploy'

subprocess.call(['killall', '-KILL', '-v', 'java'])
subprocess.call(['./setup_cluster.py', 'deployment.cfg.local'], cwd=dd)
sleep(2)
subprocess.call(['./launch_cluster.sh', 'deployment.cfg.local'], cwd=dd)
sleep(2)
subprocess.call(['./ingest.py'])
