#!/bin/bash

# Start the master
echo "starting master"
./start_master.py $1
RET=$?
if [ $RET -ne 0 ]; then
	echo "Error starting master"
	exit $RET
fi

# Start the workers
echo "starting workers"
./start_workers.py $1
RET=$?
if [ $RET -ne 0 ]; then
	echo "Error starting workers"
	exit $RET
fi
