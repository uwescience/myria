---
layout: default
title: Myria Local Installation
group: "docs"
weight: 1
section: 1
---

# Myria Local Installation

## 1. Preparation

### Install Java 8

Make sure `JAVA_HOME` points to the JDK 8 home directory on your machine.

If not, [install JDK 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) and set `JAVA_HOME` accordingly. On the Mac, you should set `JAVA_HOME` to the output of `/usr/libexec/java_home -v 1.8`:

```
$ export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)
$ echo $JAVA_HOME
/Library/Java/JavaVirtualMachines/jdk1.8.0_40.jdk/Contents/Home
```

### Install SQLite

Myria normally uses PostgreSQL as its local storage engine for production deployments, but can use [SQLite](http://www.sqlite.org/) instead in development mode. SQLite is pre-installed on many systems, but if not, most package managers support SQLite, e.g. Homebrew on the Mac:

```
brew install sqlite3
```

## 2. Setting up a local MyriaX deployment

### Download and build Myria

Ensure that `git` is installed and run `git clone https://github.com/uwescience/myria`,
which creates a directory `myria` with the `master` branch checked out.

To build the Myria jars, run `./gradlew clean shadowJar check` from within the `myria` directory. This creates a single build artifact, `build/libs/myria-0.1-all.jar`, which is deployed in the next step. Make sure this file exists before continuing.

### (Optional) Copy and edit the deployment configuration file

A MyriaX deployment needs a deployment configuration file, always at `myriadeploy/deployment.cfg`. It specifies the details of the
deployment, such as the worker hostnames and port numbers.
The `myriadeploy` directory contains some example configuration files.
For local deployment, you should use the example file `deployment.cfg.local`, which creates a local cluster with one coordinator process and two worker processes, and uses SQLite as the storage backend. You can also make your own changes to this file. If you don't want to make any changes, the `launch_local_cluster` command will automatically copy it to the right location.

If you want to make changes to the local configuration file, copy it to the standard location:

```
cp myriadeploy/deployment.cfg.local myriadeploy/deployment.cfg
```

Make any changes you want to `myriadeploy/deployment.cfg`, and then proceed to the next step.

## 3. Running the cluster

### Launch the cluster

To start the coordinator and worker processes, execute the following command from the `myriadeploy` directory:

```
./launch_local_cluster
```

You should see log output like the following:

```
INFO: REEF Version: 0.15.0
INFO  2016-07-08 14:39:15,859 [main] MyriaDriverLauncher - Submitting Myria driver to REEF...
INFO  2016-07-08 14:39:17,789 [org.apache.reef.wake.remote.impl.OrderedRemoteReceiverStage_Pull-pool-2-thread-1] MyriaDriverLauncher$RunningJobHandler - Myria driver is running...
INFO  2016-07-08 14:39:26,525 [org.apache.reef.wake.remote.impl.OrderedRemoteReceiverStage_Pull-pool-2-thread-2] MyriaDriverLauncher$JobMessageHandler - Message from Myria driver: Worker 0 ready
INFO  2016-07-08 14:39:26,527 [org.apache.reef.wake.remote.impl.OrderedRemoteReceiverStage_Pull-pool-2-thread-2] MyriaDriverLauncher$JobMessageHandler - Message from Myria driver: Master is running, starting 2 workers...
INFO  2016-07-08 14:39:31,528 [org.apache.reef.wake.remote.impl.OrderedRemoteReceiverStage_Pull-pool-2-thread-2] MyriaDriverLauncher$JobMessageHandler - Message from Myria driver: Worker 2 ready
INFO  2016-07-08 14:39:36,530 [org.apache.reef.wake.remote.impl.OrderedRemoteReceiverStage_Pull-pool-2-thread-2] MyriaDriverLauncher$JobMessageHandler - Message from Myria driver: Worker 1 ready
INFO  2016-07-08 14:39:36,532 [org.apache.reef.wake.remote.impl.OrderedRemoteReceiverStage_Pull-pool-2-thread-2] MyriaDriverLauncher$JobMessageHandler - Message from Myria driver: All 2 workers running, ready for queries...
```

### Check the cluster status

Query which workers the master knows about.

```
$ curl localhost:8753/workers
{"1":"localhost:9001","2":"localhost:9002"}
```

Query which workers are alive. They should match the output of the previous command!

```
$ curl localhost:8753/workers/alive
[1,2]
```

## 4. Using the REST API

To execute queries, we send requests to the coordinator using the coordinator's REST API.
The coordinator takes query plans in JSON format as input.

We illustrate the basic functionality using examples in the directory
`jsonQueries/getting_started`. The  `jsonQueries` directory contains additional examples.
The documentation of the full set of REST APIs can be found [here](http://docs.myriarest.apiary.io/).

### Ingest a dataset

To ingest tables that are not very large, we can send the data directly to the coordinator through the REST API.
Here we use `ingest_smallTable.json` as the example.
As specified in `ingest_smallTable.json`, it ingests the text file `smallTable` with the following schema:

    "columnTypes" : ["LONG_TYPE", "LONG_TYPE"],
    "columnNames" : ["col1", "col2"]

To ingest the file (you will need to change the path to your source data file in `ingest_smallTable.json`):

    cd jsonQueries/getting_started
    vi ./ingest_smallTable.json
    curl -i -XPOST localhost:8753/dataset -H "Content-type: application/json"  -d @./ingest_smallTable.json

To ingest a large dataset in parallel, one way is to use the `load` function in
[MyriaL](http://myria.cs.washington.edu/docs/myrial.html), which generates a parallel ingest query plan automatically.
You can also use it to ingest data from a URL, such as a location in HDFS or S3.

### Run a query

    curl -i -XPOST localhost:8753/query -H "Content-type: application/json"  -d @./global_join.json

The Datalog expression of this query is specified in `global_join.json`. The SQL equivalent is:

    SELECT t1.col1, t2.col2
    FROM smallTable AS t1, smallTable AS t2
    WHERE t1.col2 = t2.col1;

This query writes results back to the backend storage in a relation called `smallTable_join_smallTable`.
You should be able to find the resulting tables in your SQLite databases (using the `sqlite3` command-line tool). The table name is specified in the 
`DbInsert` operator, which you can modify.

### Download a dataset

    curl localhost:8753/dataset/user-jwang/program-global_join/relation-smallTable_join_smallTable/data

This will download the table `smallTable_join_smallTable` in CSV format. JSON and TSV are also supported:

    curl 'localhost:8753/dataset/user-jwang/program-global_join/relation-smallTable_join_smallTable/data?format=json'

    curl 'localhost:8753/dataset/user-jwang/program-global_join/relation-smallTable_join_smallTable/data?format=tsv'

## 5. Using the MyriaWeb interface

### Install the Google App Engine SDK

Download and install the [Google App Engine SDK](https://cloud.google.com/appengine/downloads#Google_App_Engine_SDK_for_Python). Make sure the development server exists in your `PATH`:

```
$ which dev_appserver.py
/usr/local/bin/dev_appserver.py
```

### Download and install `myria-web`

Clone the `myria-web` repository:

```
git clone https://github.com/uwescience/myria-web
```

Set up the `myria-web` application:

```
cd myria-web
git submodule update --init --recursive
pushd submodules/raco
python setup.py install
popd
```
Launch the `myria-web` application:

```
$ dev_appserver.py ./appengine                                                                                                                                                        
INFO     2016-07-08 22:57:28,888 api_server.py:204] Starting API server at: http://localhost:61792
INFO     2016-07-08 22:57:28,892 dispatcher.py:197] Starting module "default" running at: http://localhost:8080
INFO     2016-07-08 22:57:28,895 admin_server.py:118] Starting admin server at: http://localhost:8000
```

If you have a port conflict with the default port `8080`, you can specify an alternative port:

```
dev_appserver.py --port <MY_CUSTOM_PORT> ./appengine
```

###  Launch the MyriaWeb interface

Point your browser to <http://localhost:8080>. Try running the sample queries in the "Examples" tab of the right-hand pane. Click on the "Datasets" menu of the navigation bar and try downloading a dataset.

## 6. Shutting down the cluster

To shut down the cluster, simply kill the process you started in step 3 (`launch_local_cluster`). All child processes, including the coordinator and worker processes, will be automatically terminated.

# Questions and issues

If you run into a bug or limitation in Myria, feel free to [create a new issue](https://github.com/uwescience/myria/issues). For general questions, you can email the [Myria users list](mailto:myria-users@cs.washington.edu).
