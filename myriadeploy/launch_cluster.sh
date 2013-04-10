#!/bin/bash

# Start the master
echo "starting master"
./start_master.py $1
RET=$?
if [ $RET -ne 0 ]; then
	echo "failed code $RET"
	exit $RET
fi

# Sleep to ensure that the master is awake.
sleep 1

# Start the workers
echo "starting workers"
./start_workers.py $1
RET=$?
if [ $RET -ne 0 ]; then
	echo "failed code $RET"
	exit $RET
fi
