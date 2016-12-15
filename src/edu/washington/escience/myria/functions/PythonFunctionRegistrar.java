package edu.washington.escience.myria.functions;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Iterator;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.accessmethod.AccessMethod;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.accessmethod.JdbcAccessMethod;
import edu.washington.escience.myria.profiling.ProfilingLogger;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

/**
 *
 */
public class PythonFunctionRegistrar {

  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(ProfilingLogger.class);

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

    //LOGGER.info("trying to register python function");

    /* open the database connection */
    accessMethod =
        (JdbcAccessMethod) AccessMethod.of(connectionInfo.getDbms(), connectionInfo, false);
    // create table
    accessMethod.createUnloggedTableIfNotExists(
        MyriaConstants.PYUDF_RELATION, MyriaConstants.PYUDF_SCHEMA);
    udfs = new TupleBatchBuffer(MyriaConstants.PYUDF_SCHEMA);
  }

  /**
   * Add function to each worker.
   *
   * @param name function name
   * @param binary binary function
   * @param outputSchema outputSchema
   * @throws DbException if any error occurs
   */
  public void addFunction(final String name, final ByteBuffer binary, final String outputSchema)
      throws DbException {

    // add UDF
    udfs.putString(0, name);
    udfs.putByteBuffer(1, binary);
    udfs.putString(2, outputSchema);

    accessMethod.tupleBatchInsert(MyriaConstants.PYUDF_RELATION, udfs.popAny());

    return;
  }

  /**
   * get function to operator.
   *
   * @param name function name
   * @return binary function
   * @throws DbException if any error occurs
   */
  public ByteBuffer getFunction(final String name) throws DbException {

    StringBuilder sb = new StringBuilder();
    sb.append("Select * from ");
    sb.append(MyriaConstants.PYUDF_RELATION.toString(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL));
    sb.append("where function_name='");
    ;
    sb.append(name);
    sb.append("'");

    //LOGGER.info(sb.toString());
    try {
      Iterator<TupleBatch> tuples =
          accessMethod.tupleBatchIteratorFromQuery(sb.toString(), MyriaConstants.PYUDF_SCHEMA);

      if (tuples.hasNext()) {
        final TupleBatch tb = tuples.next();
        //LOGGER.info("Got {} tuples", tb.numTuples());
        if (tb.numTuples() > 0) {
          //LOGGER.info("number of tuples is greater than 1");
          return tb.getByteBuffer(1, 0); // return second column of first row.
        }
      }
    } catch (Exception e) {
      //LOGGER.info(e.getMessage());
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
      LOGGER.warn("Error checking connection validity", e);
      return false;
    }
  }
}
