package edu.washington.escience.myria.functions;

import java.sql.SQLException;
import java.util.Iterator;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.accessmethod.AccessMethod;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.accessmethod.JdbcAccessMethod;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

/**
 *
 */
public class PythonFunctionRegistrar {

  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(PythonFunctionRegistrar.class);

  /** The connection to the database database. */
  private final JdbcAccessMethod accessMethod;
  /** Buffer for UDFs registered. */
  private final TupleBatchBuffer udfs;

  /**
   * Default constructor.
   *
   * @param connectionInfo connection information
   *
   * @throws DbException if any error occurs
   */
  public PythonFunctionRegistrar(final ConnectionInfo connectionInfo) throws DbException {
    Preconditions.checkArgument(
        connectionInfo.getDbms().equals(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL),
        "Profiling only supported with Postgres JDBC connection");

    // LOGGER.info("trying to register a function");

    /* open the database connection */
    accessMethod =
        (JdbcAccessMethod) AccessMethod.of(connectionInfo.getDbms(), connectionInfo, false);
    // create table
    accessMethod.createUnloggedTableIfNotExists(
        MyriaConstants.PYFUNCTION_RELATION, MyriaConstants.PYFUNCTION_RELATION_SCHEMA);
    udfs = new TupleBatchBuffer(MyriaConstants.PYFUNCTION_RELATION_SCHEMA);
  }

  /**
   *
   * @param name - python function name to add
   * @param binary - encoded binary
   * @throws DbException
   */
  public void addUDF(final String name, final String binary) throws DbException {

    String tableName =
        MyriaConstants.PYFUNCTION_RELATION.toString(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL);

    StringBuilder sb = new StringBuilder();
    sb.append("DELETE FROM ");
    sb.append(tableName);
    sb.append(" where udfname='");
    ;
    sb.append(name);
    sb.append("'");
    String sql = sb.toString();

    accessMethod.executeSQLCommand(sql);

    udfs.putString(0, name);
    udfs.putString(1, binary);

    accessMethod.tupleBatchInsert(MyriaConstants.PYFUNCTION_RELATION, udfs.popAny());

    return;
  }

  /**
   *
   * @param name - name of python function to retrieve.
   * @return
   * @throws DbException
   */
  public String getUDF(final String name) throws DbException {

    StringBuilder sb = new StringBuilder();
    sb.append("Select * from ");
    sb.append(
        MyriaConstants.PYFUNCTION_RELATION.toString(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL));
    sb.append("where udfname='");
    ;
    sb.append(name);
    sb.append("'");

    try {
      Iterator<TupleBatch> tuples =
          accessMethod.tupleBatchIteratorFromQuery(
              sb.toString(), MyriaConstants.PYFUNCTION_RELATION_SCHEMA);

      if (tuples.hasNext()) {
        final TupleBatch tb = tuples.next();
        // LOGGER.info("Got {} tuples", tb.numTuples());
        if (tb.numTuples() > 0) {
          String codeString = tb.getString(1, 0);

          return codeString;
        }
      }
    } catch (Exception e) {
      LOGGER.info(e.getMessage());
      throw new DbException(e);
    }

    return null;
  };

  /**
   * Returns {@code true} if the current JDBC connection is active.
   *
   * @return {@code true} if the current JDBC connection is active.
   */
  public boolean isValid() {
    try {
      return accessMethod.getConnection().isValid(1);
    } catch (SQLException e) {
      LOGGER.warn("Error checking connection validity", e);
      return false;
    }
  }
}
