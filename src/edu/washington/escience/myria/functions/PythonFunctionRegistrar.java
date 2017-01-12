package edu.washington.escience.myria.functions;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Iterator;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaConstants.FunctionLanguage;
import edu.washington.escience.myria.accessmethod.AccessMethod;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.accessmethod.JdbcAccessMethod;
import edu.washington.escience.myria.api.encoding.FunctionStatus;
import edu.washington.escience.myria.profiling.ProfilingLogger;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

/**
 *This class sets and gets python functions on a postgres instance on a worker.
 */
public class PythonFunctionRegistrar {

  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(PythonFunctionRegistrar.class);

  /** The connection to the database database. */
  private final JdbcAccessMethod accessMethod;
  /** Buffer for UDFs registered. */
  private final TupleBatchBuffer pyFunctions;

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

    /* open the database connection */
    accessMethod =
        (JdbcAccessMethod) AccessMethod.of(connectionInfo.getDbms(), connectionInfo, false);
    // create table
    accessMethod.createUnloggedTableIfNotExists(
        MyriaConstants.PYUDF_RELATION, MyriaConstants.PYUDF_SCHEMA);
    pyFunctions = new TupleBatchBuffer(MyriaConstants.PYUDF_SCHEMA);
  }

  /**
   * Add function to each worker.
   *
   * @param name function name
   * @param description  of function
   * @param outputType of function
   * @param isMultivalued does function return multiple tuples.
   * @param binary binary function
   * @throws DbException if any error occurs
   */
  public void addFunction(
      final String name,
      final String description,
      final String outputType,
      final Boolean isMultivalued,
      final String binary)
      throws DbException {
    String tableName =
        MyriaConstants.PYUDF_RELATION.toString(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL);

    StringBuilder sb = new StringBuilder();
    sb.append("DELETE FROM ");
    sb.append(tableName);
    sb.append(" where function_name='");
    ;
    sb.append(name);
    sb.append("'");
    String sql = sb.toString();

    accessMethod.execute(sql);

    // add UDF
    pyFunctions.putString(0, name);
    pyFunctions.putString(1, description);
    pyFunctions.putString(2, outputType);
    pyFunctions.putBoolean(3, isMultivalued);
    pyFunctions.putString(4, binary);

    accessMethod.tupleBatchInsert(MyriaConstants.PYUDF_RELATION, pyFunctions.popAny());

    return;
  }

  /**
   * get function to operator.
   *
   * @param pyFunctionName function name
   * @return binary function
   * @throws DbException if any error occurs
   */
  public FunctionStatus getFunctionStatus(final String pyFunctionName) throws DbException {

    StringBuilder sb = new StringBuilder();
    sb.append("Select * from ");
    sb.append(MyriaConstants.PYUDF_RELATION.toString(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL));
    sb.append(" where function_name='");
    sb.append(pyFunctionName);
    sb.append("'");
    try {
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
    } catch (Exception e) {
      throw new DbException(e);
    }
    return null;
  };

  /**
   *
   * @return {@code true} if the current JDBC connection is active.
   */
  public boolean isValid() {
    try {
      return accessMethod.getConnection().isValid(1);
    } catch (SQLException e) {
      LOGGER.info("Error checking connection validity", e);
      return false;
    }
  }
  /**
   * does the function return multiple tuples?
   * @param pyFunctionName name pf the python function
   * @return true if the function returns multiple tuples.
   * @throws DbException in case of error.
   */
  public String getFunctionBinary(final String pyFunctionName) throws DbException {
  
    StringBuilder sb = new StringBuilder();
    sb.append("Select * from ");
    sb.append(MyriaConstants.PYUDF_RELATION.toString(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL));
    sb.append(" where function_name='");
    sb.append(pyFunctionName);
    sb.append("'");
    try {
      Iterator<TupleBatch> tuples =
          accessMethod.tupleBatchIteratorFromQuery(sb.toString(), MyriaConstants.PYUDF_SCHEMA);

      if (tuples.hasNext()) {
        final TupleBatch tb = tuples.next();
        if (tb.numTuples() > 0) {
          String codename = tb.getString(4, 0);
          return codename;
        }
      }

    } catch (Exception e) {
      throw new DbException(e);
    }
    return null;
  }
}
