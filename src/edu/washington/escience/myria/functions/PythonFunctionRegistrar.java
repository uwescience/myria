package edu.washington.escience.myria.functions;

import java.sql.SQLException;
import java.util.Iterator;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaConstants.FunctionLanguage;
import edu.washington.escience.myria.accessmethod.AccessMethod;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.accessmethod.JdbcAccessMethod;
import edu.washington.escience.myria.api.encoding.FunctionStatus;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

/**
 * This class sets and gets python functions on a postgres instance on a worker.
 */
public class PythonFunctionRegistrar {

  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(PythonFunctionRegistrar.class);

  /** The connection to the database database. */
  private JdbcAccessMethod accessMethod;
  /** Buffer for UDFs registered. */
  private final TupleBatchBuffer pyFunctions;
  /** connection information for reconnection if connection is closed. */
  private final ConnectionInfo connectionInfo;

  /**
   * Default constructor.
   *
   * @param connectionInfo connection information
   * @throws DbException if any error occurs
   */
  public PythonFunctionRegistrar(final ConnectionInfo connectionInfo) throws DbException {
    Preconditions.checkArgument(
        connectionInfo.getDbms().equals(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL),
        "Python functions only supported with Postgres JDBC connection");

    this.connectionInfo = connectionInfo;
    connect();

    // create table
    accessMethod.createUnloggedTableIfNotExists(
        MyriaConstants.PYUDF_RELATION, MyriaConstants.PYUDF_SCHEMA);

    pyFunctions = new TupleBatchBuffer(MyriaConstants.PYUDF_SCHEMA);
  }

  /** Helper function to connect for storing and retrieving UDFs. */
  private void connect() throws DbException {
    /* open the database connection */
    this.accessMethod =
        (JdbcAccessMethod) AccessMethod.of(connectionInfo.getDbms(), connectionInfo, false);
  }

  /**
   * Add function to current worker.
   *
   * @param name function name
   * @param description of function
   * @param outputType of function
   * @param isMultiValued does function return multiple tuples.
   * @param binary binary function
   * @throws DbException if any error occurs
   */
  public void addFunction(
      final String name,
      final String description,
      final String outputType,
      final Boolean isMultiValued,
      final String binary)
      throws DbException {
    if (!isValid()) {
      connect();
    }

    String tableName =
        MyriaConstants.PYUDF_RELATION.toString(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL);

    StringBuilder sb = new StringBuilder();
    sb.append("DELETE FROM ");
    sb.append(tableName);
    sb.append(" where function_name='");
    sb.append(name);
    sb.append("'");
    String sql = sb.toString();

    accessMethod.execute(sql);

    // add UDF
    pyFunctions.putString(0, name);
    pyFunctions.putString(1, description);
    pyFunctions.putString(2, outputType);
    pyFunctions.putBoolean(3, isMultiValued);
    pyFunctions.putString(4, binary);

    accessMethod.tupleBatchInsert(MyriaConstants.PYUDF_RELATION, pyFunctions.popAny());
  }

  /**
   * get function to operator.
   *
   * @param pyFunctionName function name
   * @return FunctionStatus function status object
   * @throws DbException if any error occurs
   */
  public FunctionStatus getFunctionStatus(final String pyFunctionName) throws DbException {
    if (!isValid()) {
      connect();
    }

    StringBuilder sb = new StringBuilder();
    sb.append("Select * from ");
    sb.append(MyriaConstants.PYUDF_RELATION.toString(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL));
    sb.append(" where function_name='");
    sb.append(pyFunctionName);
    sb.append("'");
    Iterator<TupleBatch> tuples =
        accessMethod.tupleBatchIteratorFromQuery(sb.toString(), MyriaConstants.PYUDF_SCHEMA);

    if (tuples.hasNext()) {
      final TupleBatch tb = tuples.next();
      if (tb.numTuples() > 0) {
        FunctionStatus fs =
            new FunctionStatus(
                pyFunctionName,
                tb.getString(1, 0),
                tb.getString(2, 0),
                tb.getBoolean(3, 0),
                FunctionLanguage.PYTHON,
                tb.getString(4, 0));
        return fs;
      }
    }
    return null;
  };

  /**
   * @return {@code true} if the current JDBC connection is active.
   */
  public boolean isValid() {
    try {
      return accessMethod.getConnection().isValid(1);
    } catch (SQLException e) {
      LOGGER.debug("Error checking connection validity", e);
      return false;
    }
  }
}
