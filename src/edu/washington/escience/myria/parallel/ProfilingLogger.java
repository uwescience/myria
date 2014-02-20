package edu.washington.escience.myria.parallel;

import java.io.File;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.Tuple;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.accessmethod.AccessMethod;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.accessmethod.SQLiteInfo;

/**
 * A logger for profiling data.
 */
public class ProfilingLogger {
  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(QuerySubTreeTask.class.getName());

  /** The connection to the database database. */
  private final AccessMethod accessMethod;

  /** The information for the database connection. */
  private ConnectionInfo connectionInfo;

  /** TupleBartchBuffer to collect profiling data. */
  private TupleBatchBuffer tbb;

  /** Tuple that is used to insert single rows. */
  private final Tuple tuple;

  /** The singleton instance. */
  private static ProfilingLogger instance = null;

  /**
   * Default constructor.
   *
   * @param execEnvVars execution environment variables
   *
   * @throws DbException if any error occurs
   */
  protected ProfilingLogger(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    /* retrieve connection information from the environment variables, if not already set */
    if (connectionInfo == null && execEnvVars != null) {
      connectionInfo = (ConnectionInfo) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_DATABASE_CONN_INFO);
    }

    if (connectionInfo == null) {
      throw new DbException("Unable to instantiate DbInsert: connection information unknown");
    }

    if (connectionInfo instanceof SQLiteInfo) {
      /* Set WAL in the beginning. */
      final File dbFile = new File(((SQLiteInfo) connectionInfo).getDatabaseFilename());
      SQLiteConnection conn = new SQLiteConnection(dbFile);
      try {
        conn.open(true);
        conn.exec("PRAGMA journal_mode=WAL;");
      } catch (SQLiteException e) {
        e.printStackTrace();
      }
      conn.dispose();
    }

    /* open the database connection */
    accessMethod = AccessMethod.of(connectionInfo.getDbms(), connectionInfo, false);

    accessMethod.createTableIfNotExists(MyriaConstants.PROFILING_RELATION, MyriaConstants.PROFILING_SCHEMA);
    tbb = new TupleBatchBuffer(MyriaConstants.PROFILING_SCHEMA);
    tuple = new Tuple(MyriaConstants.PROFILING_SCHEMA);
  }

  /**
   * Default constructor.
   *
   * @param execEnvVars execution environment variables
   * @return the logger instance
   */
  public static synchronized ProfilingLogger getLogger(final ImmutableMap<String, Object> execEnvVars) {
    if (instance == null) {
      try {
        instance = new ProfilingLogger(execEnvVars);
      } catch (DbException e) {
        if (LOGGER.isErrorEnabled()) {
          LOGGER.error("Failed to initialize profiling logger:", e);
        }
      }
    }
    Preconditions.checkArgument(instance.connectionInfo == (ConnectionInfo) execEnvVars
        .get(MyriaConstants.EXEC_ENV_VAR_DATABASE_CONN_INFO), "Connection info should not change.");
    return instance;
  }

  /**
   * @param queryId the query id
   * @param operatorName the operator name
   * @param fragmentId the fragment id
   * @param nanoTime the time in nanoseconds
   * @param longData integer payload
   * @param stringData string payload
   * @throws DbException if insertion in the database fails
   */
  public synchronized void recordEvent(final long queryId, final String operatorName, final long fragmentId,
      final long nanoTime, final long longData, final String stringData) throws DbException {

    Preconditions.checkArgument(tbb.numColumns() == 7);
    tbb.putLong(0, queryId);
    tbb.putInt(1, 1);
    tbb.putString(2, operatorName);
    tbb.putLong(3, fragmentId);
    tbb.putLong(4, nanoTime);
    tbb.putLong(5, longData);
    tbb.putString(6, stringData);

    TupleBatch tb = tbb.popAnyUsingTimeout();
    if (tb != null) {
      accessMethod.tupleBatchInsert(MyriaConstants.PROFILING_RELATION, MyriaConstants.PROFILING_SCHEMA, tb);
    }
  }

  /**
   * Flush the tuple batch buffer.
   *
   * @throws DbException if insertion in the database fails
   */
  public synchronized void flush() throws DbException {
    while (tbb.numTuples() > 0) {
      TupleBatch tb = tbb.popAny();
      if (tb != null) {
        accessMethod.tupleBatchInsert(MyriaConstants.PROFILING_RELATION, MyriaConstants.PROFILING_SCHEMA, tb);
      }
    }
    tbb = new TupleBatchBuffer(MyriaConstants.PROFILING_SCHEMA);
  }

  /**
   * @param queryId the query id
   * @param className the operator name
   * @param fragmentId the fragment id
   * @param nanoTime the time in nanoseconds
   * @param longData integer payload
   * @param stringData string payload
   */
  public synchronized void recordSync(final long queryId, final String className, final long fragmentId,
      final long nanoTime, final long longData, final String stringData) {

    tuple.set(0, queryId);
    tuple.set(1, 1);
    tuple.set(2, className);
    tuple.set(3, fragmentId);
    tuple.set(4, nanoTime);
    tuple.set(5, longData);
    tuple.set(6, stringData);

    try {
      accessMethod.tupleInsert(MyriaConstants.PROFILING_RELATION, MyriaConstants.PROFILING_SCHEMA, tuple);
    } catch (DbException e) {
      LOGGER.error("Failed to write profiling data:", e);
    } catch (IllegalArgumentException e) {
      e.printStackTrace();
      LOGGER.error("error", e);
    }
  }
}
