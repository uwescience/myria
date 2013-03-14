echo "dispatching, gonna take a while"
./setup_cluster.py $1 $2 $3
echo "done"
echo "starting master"
./start_master.py $1 $2 $3
echo "done"
echo "starting workers"
./start_workers.py $1 $2 $3
echo "done"
