---
layout: default
title: Local Installation
group: "docs"
weight: 1
section: 2
---

# MyriaX Engine

## Local Installation

### 1. Preparation

#### Java Version

Make sure `java -version` shows `7` on your machine.

If not, you can put Java 7 in your directory, and let your `PATH` include it **before** the default `PATH`:

    export PATH=(path_to_java7_bin_folder):$PATH

#### Passwordless SSH

You need to be able to do `ssh localhost` without typing your password.

 - Start SSH Server on your machine to enable remote login. Instructions
for how to do this can be found [here](http://osxdaily.com/2011/09/30/remote-login-ssh-server-mac-os-x/) for Mac
and [here](https://help.ubuntu.com/community/SSH/OpenSSH/Configuring) for Ubuntu.

 - Setting up keys. If you have not setup keys before, the easiest way to do it is as follows:

    `ssh-keygen`

    `ssh-copy-id username@localhost`

Use default settings. You may need to install `ssh-copy-id` if you don't have it on your machine.
Instructions for setting up keys without installing ssh-copy-id can be found [here](http://osxdaily.com/2012/05/25/how-to-set-up-a-password-less-ssh-login/).

To test, run `ssh localhost`.

#### Storage

You need to install [SQLite](http://www.sqlite.org/), which is already pre-installed on many systems.
For data storage, MyriaX uses existing single-node relational database management systems.
You can still use SQLite, but the preferred system is [PostgreSQL](http://www.postgresql.org/).
Myria currently uses Postgres 9.4, and installation instructions can be founded [here](http://www.postgresql.org/download/).

### 2. Setting up a local MyriaX deployment

#### Download and Build Myria

We suggest that you use `git` because you may want to switch between branches later, although GitHub also offers a ZIP file of the master branch.
To do that, install `git` and run `git clone https://github.com/uwescience/myria`,
which creates a directory `myria` with the master branch inside.

To build the Myria jars, run `./gradlew jar` from within the `myria` directory,.

Note: if it is not a fresh installation (e.g. you just switched from another branch),
you may need to run `./gradlew clean` before `./gradlew jar`. This is for cleaning different versions of Java libraries.

If the build succeeded, you should be able to see jars in `build/libs` including `myria-0.1.jar`.

#### Deployment configuration file

A MyriaX deployment needs a deployment config file. It specifies the details of the
deployment to start with, such as the worker hostnames and port numbers.
The `myriadeploy` directory contains some example configuration files.
In this doc, we will be using `deployment.cfg.local` as a starting point, which creates
a deployment with one coordinator process and two worker processes, and uses
SQLite as the storage backend. You can also make your own changes to it.

If you want to use [PostgreSQL](www.postgresql.org),
`deployment.cfg.postgres` shows how to set up the configuration file.
You need to take these additional steps:

- Create a `uwdb` role in all your Postgres instances which Myria will use to manage the
tables stored in Postgres.
    ```sql
    create role uwdb;
    alter role uwdb with login;
    ```

- Create Postgres databases. Important: If you have multiple workers
on the same machine, they need to use different Postgres databases
(but they can share the same Postgres server instance). For example,
the configuration in `deployment.cfg.postgres` needs both Postgres
databases `myria1` and `myria2` on both worker machines:
    ```sql
    createdb myria1;
    createdb myria2;
    ```
You can replace `myria1` and `myria2` with your own databases.

- (Optional) Should you wish Myria to connect via an alternate port, add the
following key/value pair to the `[deployment]` section of your Myria deployment file:
    ```
    database_port = [custom_port_number]
    ```

### 3. Running the cluster

#### Setup the working directories and catalogs

Before we can launch the cluster and run queries, we need to setup the catalog and
the working directories. These initialization steps are automated and executed by the `setup_cluster.py` script.

To initialize (or re-initialize) a new Myria cluster configuration, execute the following command:

    ./setup_cluster.py deployment.cfg.local --clean-catalog

This will create the catalog and the working directories in `/tmp/myria/twoNodeLocalParallel".
Notice that this **overwrites** the catalogs: all information about previously ingested relations will be removed.
If you only want to update the jars without losing information in the catalog, run:

    ./setup_cluster.py deployment.cfg.local

#### Launch the cluster

To start the master and the worker processes, execute the following command

    ./launch_cluster.sh deployment.cfg.local

This command will output things like the following:

    starting master
    ...
    Waiting for the master to be up...

If everything is okay, it will start the workers:

    starting workers
    2 = localhost
    1 = localhost

#### Check the cluster status

- Query which workers the master knows about.

    curl -i localhost:8753/workers

- Query which workers are alive. They better match what's in `deployment.cfg.local`!

    curl -i localhost:8753/workers/alive

### 4. Using the cluster

To execute queries, we send requests to the coordinator using the coordinator's REST API.
The coordinator takes query plans in JSON format as input.

We illustrate the basic functionality using examples in the directory
`jsonQueries/getting_started`. The  `jsonQueries` directory contains additional examples.
The documentation of the full set of REST APIs can be founded [here](http://docs.myriarest.apiary.io/).

#### Ingest some data.

To ingest tables that are not very large, we can send the data directly to the coordinator through the REST API.
Here we use `ingest_smallTable.json` as the example.
As specified in `ingest_smallTable.json`, it ingests the text file `smallTable` with the following schema:

    "columnTypes" : ["LONG_TYPE", "LONG_TYPE"],
    "columnNames" : ["col1", "col2"]

To ingest it:

    curl -i -XPOST localhost:8753/dataset -H "Content-type: application/json"  -d @./ingest_smallTable.json

You may need to change the path to your source data file in `ingest_smallTable.json`.

To ingest a large dataset in parallel, one way is to use the `load` function in
[MyriaL](http://myria.cs.washington.edu/docs/myriaql.html), which generates a parallel ingest query plan automatically.
You can also use it to ingest data from an URL, for example, a location in HDFS or S3.

#### Run a query.

    curl -i -XPOST localhost:8753/query -H "Content-type: application/json"  -d @./global_join.json

The Datalog expression of this query is specified in `global_join.json`. The SQL equivalence is:

    Select t1.col1, t2.col2
    From smallTable as t1, smallTable as t2
    Where t1.col2 = t2.col1;

This query writes results back to the backend storage in a relation called `smallTable_join_smallTable`.
You should be able to find the resulting tables in your databases. The table name is specified in the 
`DbInsert` operator, which you can modify.

#### Download a dataset.

    curl -i localhost:8753/dataset/user-jwang/program-global_join/relation-smallTable_join_smallTable/data

This will download the table `smallTable_join_smallTable` in CSV format. JSON and TSV are also supported, to do that, specify the format as the following:

    curl -i localhost:8753/dataset/user-jwang/program-global_join/relation-smallTable_join_smallTable/data?format=json

### 5. Shutdown the cluster

Shutdown the whole cluster via the REST API:

    curl -i localhost:8753/server/shutdown

This will shutdown everything, including the master and all the workers.

If there was an error in any query, ingesting data, etc., then the cluster might freeze and
most commands that involve workers (e.g., queries, ingestion, and shutdown) would not work. You can
force-quit all machines:

    ./stop_all_by_force deployment.cfg.local

This will go to all the nodes, find the master/worker processes under your username, and kill them. Alternatively, you can also restart the cluster with the following REST call:

    curl -i localhost:8753/server/restart

## Using a shared-nothing cluster

To use a shared-nothing cluster to run MyriaX instead of your local
machine, specify the cluster configuration, including the machine names,
port numbers, working directories, database names, etc, in your
`deployment.cfg` file.
See `deployment.cfg.sample` for an example.

Similar to local installation, make sure you have: Java 7,
passwordless SSH from the master machine(s) to all the worker
machine(s), Postgres users and databases created on your worker
machine(s) on your cluster.


## Questions, issues, problems

Welcome to check our [GitHub issues page](https://github.com/uwescience/myria/issues) and post your problems there. We will take care of them!

