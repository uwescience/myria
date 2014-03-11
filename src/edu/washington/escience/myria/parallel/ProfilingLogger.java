package edu.washington.escience.myria.parallel;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import scala.NotImplementedError;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.accessmethod.AccessMethod;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.accessmethod.JdbcAccessMethod;
import edu.washington.escience.myria.accessmethod.SQLiteAccessMethod;
import edu.washington.escience.myria.accessmethod.SQLiteInfo;
import edu.washington.escience.myria.operator.Operator;

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

  /** The jdbc connection. */
  private Connection connection;

  /** A query statement for batching. */
  private PreparedStatement statement;

  /** Number of rows in batch in {@link #statement}. */
  private int batchSize = 0;

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
    accessMethod.createTableIfNotExists(MyriaConstants.LOG_SENT_RELATION, MyriaConstants.LOG_SENT_SCHEMA);

    if (accessMethod instanceof JdbcAccessMethod) {
      connection = ((JdbcAccessMethod) accessMethod).getConnection();
      try {
        statement =
            connection.prepareStatement(accessMethod.insertStatementFromSchema(MyriaConstants.PROFILING_SCHEMA,
                MyriaConstants.PROFILING_RELATION));
      } catch (SQLException e) {
        throw new DbException(e);
      }

    } else if (accessMethod instanceof SQLiteAccessMethod) {
      throw new NotImplementedError();
    } else {
      throw new NotImplementedError();
    }
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
   * Inserts in batches. Call {@link #flush()}.
   * 
   * @param operator the operator where this record was logged
   * @param numTuples the number of tuples
   * @param eventType the type of the event to be logged
   * @throws DbException if insertion in the database fails
   */
  public synchronized void recordEvent(final Operator operator, final long numTuples, final String eventType)
      throws DbException {

    try {

      statement.setLong(1, operator.getQueryId());
      statement.setString(2, operator.getOpName());
      statement.setLong(3, operator.getFragmentId());
      statement.setLong(4, getTime(operator));
      statement.setLong(5, numTuples);
      statement.setString(6, eventType);

      statement.addBatch();
      batchSize++;
    } catch (final SQLException e) {
      throw new DbException(e);
    }

    if (batchSize > TupleBatch.BATCH_SIZE) {
      flush();
    }
  }

  /**
   * Returns the relative time to the beginning on the query in nanoseconds.
   * 
   * The time that we log is the relative time to the beginning of the query in nanoseconds (workerStartTimeMillis). We
   * assume that all workers get initialized at the same time so that the time in ms recorded on the workers can be
   * considered 0. The time in nanoseconds is only a relative time with regard to some arbitrary beginning and it is
   * different of different threads. This means we have to calculate the difference on the thread and add it to the
   * difference in ms between the worker and the thread (startupTimeMillis).
   * 
   * @param operator the operator
   * @return the time to record
   */
  private long getTime(final Operator operator) {
    final WorkerQueryPartition workerQueryPartition = (WorkerQueryPartition) operator.getQueryPartition();
    final long workerStartTimeMillis = workerQueryPartition.getBeginMilliseconds();
    final long threadStartTimeMillis = operator.getSubTreeTask().getBeginMilliseconds();
    final long startupTimeMillis = threadStartTimeMillis - workerStartTimeMillis;
    Preconditions.checkArgument(startupTimeMillis >= 0);
    final long threadStartNanos = operator.getSubTreeTask().getBeginNanoseconds();
    final long activeTimeNanos = System.nanoTime() - threadStartNanos;

    return TimeUnit.MILLISECONDS.toNanos(startupTimeMillis) + activeTimeNanos;
  }

  /**
   * Flush the tuple batch buffer.
   * 
   * @throws DbException if insertion in the database fails
   */
  public synchronized void flush() throws DbException {
    try {
      if (batchSize > 0) {
        statement.executeBatch();
        statement.clearBatch();
        batchSize = 0;
      }
    } catch (SQLException e) {
      throw new DbException(e);
    }
  }

  /**
   * Record that data was sent to a worker.
   * 
   * @param operator the operator where this record was logged
   * @param numTuples the number of tuples sent.
   * @param destWorkerId the worker if that we send the data to
   */
  public void recordSend(final Operator operator, final int numTuples, final int destWorkerId) {
    try {
      final PreparedStatement singleStatement =
          connection.prepareStatement(accessMethod.insertStatementFromSchema(MyriaConstants.LOG_SENT_SCHEMA,
              MyriaConstants.LOG_SENT_RELATION));

      singleStatement.setLong(1, operator.getQueryId());
      singleStatement.setLong(2, operator.getFragmentId());
      singleStatement.setLong(3, getTime(operator));
      singleStatement.setLong(4, numTuples);
      singleStatement.setInt(5, destWorkerId);

      singleStatement.executeUpdate();
      singleStatement.close();
    } catch (final SQLException e) {
      LOGGER.error("Failed to write profiling data:", e);
    }
  }
}
