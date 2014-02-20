package edu.washington.escience.myria.parallel;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

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

    try {
      statement.setLong(1, queryId);
      statement.setInt(2, 1);
      statement.setString(3, operatorName);
      statement.setLong(4, fragmentId);
      statement.setLong(5, nanoTime);
      statement.setLong(6, longData);
      statement.setString(7, stringData);
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
   * Directly inserts entry.
   * 
   * @param queryId the query id
   * @param className the operator name
   * @param fragmentId the fragment id
   * @param nanoTime the time in nanoseconds
   * @param longData integer payload
   * @param stringData string payload
   */
  public synchronized void recordSync(final long queryId, final String className, final long fragmentId,
      final long nanoTime, final long longData, final String stringData) {
    try {
      final PreparedStatement singleStatement =
          connection.prepareStatement(accessMethod.insertStatementFromSchema(MyriaConstants.PROFILING_SCHEMA,
              MyriaConstants.PROFILING_RELATION));
      singleStatement.setLong(1, queryId);
      singleStatement.setInt(2, 1);
      singleStatement.setString(3, className);
      singleStatement.setLong(4, fragmentId);
      singleStatement.setLong(5, nanoTime);
      singleStatement.setLong(6, longData);
      singleStatement.setString(7, stringData);
      singleStatement.executeUpdate();
      singleStatement.close();
    } catch (final SQLException e) {
      LOGGER.error("Failed to write profiling data:", e);
    }
  }
}
