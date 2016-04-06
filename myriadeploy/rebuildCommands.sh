cd ../ 
./gradlew jar
cd myriadeploy/
./kill_all_java_processes.py deployment.cfg.local 
./stop_all_by_force.py deployment.cfg.local 
./update_myria_binaries.py deployment.cfg.local 
./launch_cluster.sh deployment.cfg.local 