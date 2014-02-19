package edu.washington.escience.myria.parallel;

import java.io.File;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.accessmethod.AccessMethod;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.accessmethod.SQLiteInfo;

/**
 * A logger for profiling data.
 */
public class ProfilingLogger {

  /** The connection to the database database. */
  private final AccessMethod accessMethod;

  /** The information for the database connection. */
  private ConnectionInfo connectionInfo;

  /** TupleBartchBuffer to collect profiling data. */
  private final TupleBatchBuffer tbb;

  /**
   * Default constructor.
   *
   * @param execEnvVars execution environment variables
   *
   * @throws DbException if any error occurs
   */
  public ProfilingLogger(final ImmutableMap<String, Object> execEnvVars) throws DbException {
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
  public void recordEvent(final long queryId, final String operatorName, final long fragmentId, final long nanoTime,
      final long longData, final String stringData) throws DbException {

    tbb.putLong(0, queryId);
    tbb.putInt(1, 1);
    tbb.putString(2, operatorName);
    tbb.putLong(3, fragmentId);
    tbb.putLong(4, nanoTime);
    tbb.putLong(5, longData);
    tbb.putString(6, stringData);

    TupleBatch tb = tbb.popAny();
    if (tb != null) {
      accessMethod.tupleBatchInsert(MyriaConstants.PROFILING_RELATION, MyriaConstants.PROFILING_SCHEMA, tb);
    }
  }
}
