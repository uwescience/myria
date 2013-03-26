#!/bin/bash

# Start the master
echo "starting master"
./start_master.py $1 $2 $3 $4
RET=$?
if [ $RET -ne 0 ]; then
	echo "failed code $RET"
	exit $RET
fi

# Start the workers
echo "starting workers"
./start_workers.py $1 $2 $3 $4
RET=$?
if [ $RET -ne 0 ]; then
	echo "failed code $RET"
	exit $RET
fi
