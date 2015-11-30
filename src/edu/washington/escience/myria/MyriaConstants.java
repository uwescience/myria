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
	 * Execution mode, see
	 * {@link edu.washington.escience.myria.parallel.QueryExecutionMode}.
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

	/**
	 * Default value for
	 * {@link MyriaSystemConfigKeys#FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES}.
	 */
	public static final int FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES_DEFAULT_VALUE = 5 * MB;

	/**
	 * Default value for
	 * {@link MyriaSystemConfigKeys#FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES}.
	 */
	public static final int FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES_DEFAULT_VALUE = 512 * KB;

	/** Time interval between two heartbeats. */
	public static final int HEARTBEAT_INTERVAL = 1000;

	/** The identity of the master worker is current always zero. */
	public static final int MASTER_ID = 0;

	/**
	 * Timeout for master process startup.
	 */
	public static final int MASTER_START_UP_TIMEOUT_IN_SECOND = 20;

	/**
	 * Default value for
	 * {@link MyriaSystemConfigKeys#OPERATOR_INPUT_BUFFER_CAPACITY}.
	 */
	public static final int OPERATOR_INPUT_BUFFER_CAPACITY_DEFAULT_VALUE = 100;

	/**
	 * Default value for
	 * {@link MyriaSystemConfigKeys#OPERATOR_INPUT_BUFFER_RECOVER_TRIGGER}.
	 */
	public static final int OPERATOR_INPUT_BUFFER_RECOVER_TRIGGER_DEFAULT_VALUE = 80;

	/** timeout of returning a tuple batch even not filled. */
	public static final long PUSHING_TB_TIMEOUT = 1000000000;

	/**
	 * How long do we treat a scheduled new worker as failed to start, in
	 * milliseconds.
	 */
	public static final long SCHEDULED_WORKER_FAILED_TO_START = 5000;

	/**
	 * How long do we treat a scheduled new worker as unable to start, in
	 * milliseconds.
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

	/**
	 * PostgreSQL port.
	 */
	public static final int STORAGE_POSTGRESQL_PORT = 5401;

	/**
	 * Default value for
	 * {@link MyriaSystemConfigKeys#TCP_CONNECTION_TIMEOUT_MILLIS}.
	 */
	public static final int TCP_CONNECTION_TIMEOUT_MILLIS_DEFAULT_VALUE = 3000;

	/**
	 * Default value for
	 * {@link MyriaSystemConfigKeys#TCP_RECEIVE_BUFFER_SIZE_BYTES}.
	 */
	public static final int TCP_RECEIVE_BUFFER_SIZE_BYTES_DEFAULT_VALUE = 2 * MB;

	/**
	 * Default value for
	 * {@link MyriaSystemConfigKeys#TCP_SEND_BUFFER_SIZE_BYTES}.
	 */
	public static final int TCP_SEND_BUFFER_SIZE_BYTES_DEFAULT_VALUE = 5 * MB;

	/**
	 * If a thread in a thread pool is idle, how long it should wait before
	 * terminates itself. Currently, 5 minutes.
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
	 * The time interval in milliseconds for check if the worker should be
	 * shutdown.
	 */
	public static final int WORKER_SHUTDOWN_CHECKER_INTERVAL = 1000;

	/**
	 * Default value for
	 * {@link MyriaSystemConfigKeys#WORKER_STORAGE_DATABASE_SYSTEM}.
	 */
	public static final String WORKER_STORAGE_DATABASE_SYSTEM_DEFAULT_VALUE = STORAGE_SYSTEM_SQLITE;

	/**
	 * The number of bytes that can back up in a
	 * {@link java.io.PipedInputStream} before we stop writing tuples and wait
	 * for the client to read them. 16 MB.
	 */
	public static final int DEFAULT_PIPED_INPUT_STREAM_SIZE = 1024 * 1024 * 16;

	/**
	 * The maximum number of currently active (running, queued, paused, ...)
	 * queries at the master.
	 */
	public static final int MAX_ACTIVE_QUERIES = 25;

	/**
	 * The relation that stores profiling information.
	 */
	public static final RelationKey PROFILING_RELATION = new RelationKey(
			"public", "logs", "Profiling");

	/**
	 * The schema of the {@link #PROFILING_RELATION}.
	 */
	public static final Schema PROFILING_SCHEMA = Schema.ofFields(
			Type.LONG_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.LONG_TYPE,
			Type.LONG_TYPE, Type.LONG_TYPE, "queryId", "fragmentId", "opId",
			"startTime", "endTime", "numTuples");

	/**
	 * The relation that stores profiling information.
	 */
	public static final RelationKey SENT_RELATION = new RelationKey("public",
			"logs", "Sending");

	/**
	 * The schema of the {@link #SENT_RELATION}.
	 */
	public static final Schema SENT_SCHEMA = Schema.ofFields(Type.LONG_TYPE,
			Type.INT_TYPE, Type.LONG_TYPE, Type.LONG_TYPE, Type.INT_TYPE,
			"queryId", "fragmentId", "nanoTime", "numTuples", "destWorkerId");

	/**
	 * For how long cached versions of the profiling data should be valid.
	 */
	public static final long PROFILING_CACHE_AGE = TimeUnit.HOURS.toMillis(1);

	/**
	 * Number of entries in profiling logger before it is flushed.
	 */
	public static final int PROFILING_LOGGER_BATCH_SIZE = 10000;

	/**
	 * The maximum number of subqueries we will allow a query to execute before
	 * killing it. This is a safeguard against an infinite loop.
	 */
	public static final int MAXIMUM_NUM_SUBQUERIES = 1000;

	/**
	 * Default imports for janino. Modules imported here can be used in
	 * expressions.
	 */
	public static final String[] DEFAULT_JANINO_IMPORTS = {
			"com.google.common.hash.Hashing", "java.nio.charset.Charset" };

	/** Private constructor to disallow building utility class. */
	private MyriaConstants() {
	}

	/** available fault-tolerance mode for each query in Myria. */
	public static enum FTMODE {
		/** three FT modes are supported. */
		none, abandon, rejoin
	};
}
