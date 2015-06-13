#Preparing to use

#### 0. Build the Myria jar
In Myria source (`../`), run `./gradlew jar`.

#### 1. Myria needs Java 7
Make sure `java -version` shows 7 on any machines in the cluster. 

If not (e.g., support-managed machines have Java 6), you can put Java 7 in your directory, and let your `PATH` include it BEFORE the default `PATH`

    export PATH=(path to java7 bin folder):$PATH

#### 2. You need passwordless SSH to the cluster

Better make sure you can access any machine in the cluster from your client side, using `ssh` with no password. Otherwise, you will need to enter them a lot of times.

If you have not setup keys before, the easiest way to make it happen:

    ssh-keygen
    ssh-copy-id username@remote_machine_address

Use default settings all the way.

Then ssh to the remote machine and check

    ssh username@remote_machine_address

Install `ssh-copy-id` if you don't have it on your machine.

# Setting up a new cluster deployment

#### 0. Create an updated deployment configuration file

Edit the template in `deployment.cfg.sample`

    cp deployment.cfg.sample deployment.cfg
    
#### 1. Setup the cluster directories and catalogs and copy to nodes

Use this script if you want to initialize (or re-initialize) a new Myria cluster configuration.

    ./setup_cluster.py <deployment.cfg>

This will: Create a directory called `<description>` with all the catalog files, then dispatch them to corresponding machines in `<master + workers>` under `<path>`.

Notice this **overwrites** the cluster: no ingested relations in previous Myria instances will be inherited by this new one.

# Running the cluster
#### 0. Launch the cluster.
A. If you are lazy:

       ./launch_cluster.sh <deployment.cfg>

   You can run this script from any machine where you can `ssh` to every node in `master + workers`. Now you are done!

B. The `launch_cluster.sh` script really just runs the following 2 scripts in order.
    * `./start_master.py` 
    * `./start_workers.py`
    
   If you want to do it separately:
        
       ./start_master.py <deployment.cfg>

   This will remotely start the master daemon, which includes the REST API server and the "query" server. Process output `stderr` and `stdout` are redirected to corresponding files `master.stderr` and `master.stdout` in the `path` specified in the configuration file.
    
       ./start_workers.py <deployment.cfg>

   Next, start the workers.
     
       ./start_workers.py <deployment.cfg>

C. You can also launch the cluster using Eclipse.

   Edit the run configuration for `RunMyriaForProfiling` and set the working directory in *Arguments* to the Myria working directory (for example `/tmp/myria/twoNodeLocalParallel` if you are using `deployment.cfg.local`). When you hit run, the cluster is started.

#### 1. Check the cluster status.

A. Query which workers the master knows about. They better match what's in `deployment.cfg`!

       curl -i beijing:8753/workers

   Replace `beijing` with your master's hostname.

B. Query which workers are alive

       curl -i beijing:8753/workers/alive

   Replace `beijing` with your master's hostname.
    
#### 2. Start using the cluster

A. Ingest some data.

       curl -i -XPOST beijing:8753/dataset -H "Content-type: application/json"  -d @./ingest_twitter.json

   Examples are in the directory `../jsonQueries`, check them.

B. Run a query.

       curl -i -XPOST beijing:8753/query -H "Content-type: application/json"  -d @./global_join.json
        
   Examples are in the directory `../jsonQueries`, check them.
    
   Check the result in master.stdout in your working directory, if the result is sent back to the server.

The above steps are repeatable, until you are willing to let them go.

#### 3. Shutdown the cluster.

A. Shutdown the whole cluster via the REST API:

    curl -i beijing:8753/server/shutdown

   This will shutdown everything, including the master and all the workers.

B. If there was an error in any query, ingesting data, etc., then the cluster will freeze and most commands that involve workers (e.g., queries, ingestion, and shutdown) will not work.

   You can force-quit all machines:
    
       ./stop_all_by_force <deployment.cfg>
    
   This will go to all the nodes, find the master/worker processes under your username, and kill them.
