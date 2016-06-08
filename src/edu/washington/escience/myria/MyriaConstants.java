package edu.washington.escience.myria;

import java.util.concurrent.TimeUnit;

/**
 * This class holds the constants for the Myria execution.
 *
 */
public final class MyriaConstants {
  /**
   * The system name.
   */
  public static final String SYSTEM_NAME = "Myria";

  /**
   * 1 kb.
   */
  public static final int KB = 1024;

  /**
   * 1 mb.
   */
  public static final int MB = 1024 * KB;

  /**
   * 1 gb.
   */
  public static final int GB = 1024 * MB;

  /**
   * 1 tb.
   */
  public static final int TB = 1024 * GB;

  /**
   * The default port for the REST server.
   */
  public static final int DEFAULT_MYRIA_API_PORT = 8753;

  /**
   * Execution environment variable, the database name.
   */
  public static final String EXEC_ENV_VAR_DATABASE_CONN_INFO = "execEnvVar.database.conn.info";

  /**
   * Execution environment variable, the database system.
   */
  public static final String EXEC_ENV_VAR_DATABASE_SYSTEM = "execEnvVar.database.system";

  /**
   * Driving task.
   */
  public static final String EXEC_ENV_VAR_DRIVING_TASK = "drivingTask";

  /**
   * Execution mode, see {@link edu.washington.escience.myria.parallel.QueryExecutionMode}.
   */
  public static final String EXEC_ENV_VAR_EXECUTION_MODE = "executionMode";

  /**
   * Node ID.
   */
  public static final String EXEC_ENV_VAR_NODE_ID = "nodeId";

  /**
   * Task resource manager.
   */
  public static final String EXEC_ENV_VAR_FRAGMENT_RESOURCE_MANAGER = "fragmentResourceManager";

  /**
   * Query ID.
   */
  public static final String EXEC_ENV_VAR_QUERY_ID = "queryId";

  /**
   * Profiling mode.
   */
  public static final String EXEC_ENV_VAR_PROFILING_MODE = "profiling_mode";

  /** Time interval between two heartbeats. */
  public static final int HEARTBEAT_INTERVAL = 1000;

  /** Time interval between two resource usage reports. */
  public static final int RESOURCE_REPORT_INTERVAL = 1000;

  /** The identity of the master worker is current always zero. */
  public static final int MASTER_ID = 0;

  /**
   * Timeout for master process startup.
   */
  public static final int MASTER_START_UP_TIMEOUT_IN_SECOND = 20;

  /** timeout of returning a tuple batch even not filled. */
  public static final long PUSHING_TB_TIMEOUT = 1000000000;

  /**
   * How long do we treat a scheduled new worker as failed to start, in milliseconds.
   */
  public static final long SCHEDULED_WORKER_FAILED_TO_START = 5000;

  /**
   * How long do we treat a scheduled new worker as unable to start, in milliseconds.
   */
  public static final long SCHEDULED_WORKER_UNABLE_TO_START = 15000;

  /**
   * Short wait interval 1 millisecond.
   */
  public static final int SHORT_WAITING_INTERVAL_1_MS = 1;

  /**
   * Short wait interval 10 milliseconds.
   */
  public static final int SHORT_WAITING_INTERVAL_10_MS = 10;

  /**
   * Short wait interval 100 milliseconds.
   */
  public static final int SHORT_WAITING_INTERVAL_100_MS = 100;

  /**
   * Short wait interval default 100 milliseconds.
   */
  public static final int SHORT_WAITING_INTERVAL_MS = SHORT_WAITING_INTERVAL_100_MS;

  /** the database name. */
  public static final String STORAGE_DATABASE_NAME = "myria.db";

  /**
   * JDBC username.
   */
  public static final String STORAGE_JDBC_USERNAME = "uwdb";

  /**
   * MonetDB storage.
   */
  public static final String STORAGE_SYSTEM_MONETDB = "monetdb";

  /**
   * MonetDB port.
   */
  public static final int STORAGE_MONETDB_PORT = 50001;

  /**
   * Mysql storage.
   */
  public static final String STORAGE_SYSTEM_MYSQL = "mysql";

  /**
   * Mysql port.
   */
  public static final int STORAGE_MYSQL_PORT = 3301;

  /**
   * SQLite storage.
   */
  public static final String STORAGE_SYSTEM_SQLITE = "sqlite";

  /**
   * PostgreSQL storage.
   */
  public static final String STORAGE_SYSTEM_POSTGRESQL = "postgresql";

  /** Worker config file name. */
  public static final String WORKER_CONFIG_FILE = "worker.cfg";

  /** Deployment config file name. */
  public static final String DEPLOYMENT_CONF_FILE = "deployment.cfg";

  /**
   * PostgreSQL port.
   */
  public static final int STORAGE_POSTGRESQL_PORT = 5432;

  /**
   * If a thread in a thread pool is idle, how long it should wait before terminates itself. Currently, 5 minutes.
   */
  public static final int THREAD_POOL_KEEP_ALIVE_TIME_IN_MS = 1000 * 60 * 5;

  /**
   * Short wait interval in milliseconds.
   */
  public static final int WAITING_INTERVAL_1_SECOND_IN_MS = 1000;

  /** How long do we treat a worker as dead, in milliseconds. */
  public static final long WORKER_IS_DEAD_INTERVAL = 5000;

  /** How long do we wait for next worker liveness check, in milliseconds. */
  public static final long WORKER_LIVENESS_CHECKER_INTERVAL = 1000;

  /**
   * The time interval in milliseconds for check if the worker should be shutdown.
   */
  public static final int WORKER_SHUTDOWN_CHECKER_INTERVAL = 1000;

  /**
   * The number of bytes that can back up in a {@link java.io.PipedInputStream} before we stop writing tuples and wait
   * for the client to read them. 16 MB.
   */
  public static final int DEFAULT_PIPED_INPUT_STREAM_SIZE = 1024 * 1024 * 16;

  /**
   * The maximum number of currently active (running, queued, paused, ...) queries at the master.
   */
  public static final int MAX_ACTIVE_QUERIES = 25;

  /**
   * The relation that stores profiling information about which operators executed when.
   */
  public static final RelationKey EVENT_PROFILING_RELATION =
      new RelationKey("public", "logs", "Profiling");

  /**
   * The schema of the {@link #EVENT_PROFILING_RELATION}.
   */
  public static final Schema EVENT_PROFILING_SCHEMA =
      Schema.ofFields(
          "queryId",
          Type.LONG_TYPE,
          "subQueryId",
          Type.INT_TYPE,
          "fragmentId",
          Type.INT_TYPE,
          "opId",
          Type.INT_TYPE,
          "startTime",
          Type.LONG_TYPE,
          "endTime",
          Type.LONG_TYPE,
          "numTuples",
          Type.LONG_TYPE);

  /**
   * The relation that stores profiling information about sent tuples.
   */
  public static final RelationKey SENT_PROFILING_RELATION =
      new RelationKey("public", "logs", "Sending");

  /**
   * The schema of the {@link #SENT_PROFILING_RELATION}.
   */
  public static final Schema SENT_PROFILING_SCHEMA =
      Schema.ofFields(
          "queryId",
          Type.LONG_TYPE,
          "subQueryId",
          Type.INT_TYPE,
          "fragmentId",
          Type.INT_TYPE,
          "nanoTime",
          Type.LONG_TYPE,
          "numTuples",
          Type.LONG_TYPE,
          "destWorkerId",
          Type.INT_TYPE);

  /**
   * The relation that stores resource profiling information.
   */
  public static final RelationKey RESOURCE_PROFILING_RELATION =
      new RelationKey("public", "logs", "Resource");

  /**
   * The schema of the {@link #RESOURCE_PROFILING_RELATION}.
   */
  public static final Schema RESOURCE_PROFILING_SCHEMA =
      Schema.ofFields(
          "timestamp",
          Type.LONG_TYPE,
          "opId",
          Type.INT_TYPE,
          "measurement",
          Type.STRING_TYPE,
          "value",
          Type.LONG_TYPE,
          "queryId",
          Type.LONG_TYPE,
          "subqueryId",
          Type.LONG_TYPE);

  /**
   * For how long cached versions of the profiling data should be valid.
   */
  public static final long PROFILING_CACHE_AGE = TimeUnit.HOURS.toMillis(1);

  /**
   * The maximum number of subqueries we will allow a query to execute before killing it. This is a safeguard against an
   * infinite loop.
   */
  public static final int MAXIMUM_NUM_SUBQUERIES = 1000;

  /**
   * Default imports for janino. Modules imported here can be used in expressions.
   */
  public static final String[] DEFAULT_JANINO_IMPORTS = {
    "com.google.common.hash.Hashing", "java.nio.charset.Charset"
  };

  /** Private constructor to disallow building utility class. */
  private MyriaConstants() {}

  /** available fault-tolerance mode for each query in Myria. */
  public static enum FTMode {
    /** three FT modes are supported. */
    NONE,
    ABANDON,
    REJOIN
  };

  /** available profiling mode for each query in Myria. */
  public static enum ProfilingMode {
    /**
     * RESOURCE: resource usage (CPU, IO, Memory) only.
     */
    RESOURCE,
    /**
     * QUERY: query execution only.
     */
    QUERY
  };
}
