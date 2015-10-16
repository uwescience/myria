#!/bin/bash

# compile
echo "compile"
gradle -p .. jar

# stop every worker
echo "stop"
./stop_all_by_force.py $1
RET=$?
if [ $RET -ne 0 ]; then
	echo "failed code $RET"
	exit $RET
fi

# update jar
echo "update"
./setup_cluster.py $1
RET=$?
if [ $RET -ne 0 ]; then
	echo "failed code $RET"
	exit $RET
fi

# Start the workers
echo "re-launch"
./launch_cluster.sh $1
RET=$?
if [ $RET -ne 0 ]; then
	echo "failed code $RET"
	exit $RET
fi
