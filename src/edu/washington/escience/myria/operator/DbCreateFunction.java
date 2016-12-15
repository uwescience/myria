package edu.washington.escience.myria.operator;

import java.io.IOException;
import java.nio.ByteBuffer;

import sun.misc.BASE64Decoder;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaConstants.FunctionLanguage;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.accessmethod.AccessMethod;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.expression.evaluate.GenericEvaluator;
import edu.washington.escience.myria.functions.PythonFunctionRegistrar;
import edu.washington.escience.myria.storage.TupleBatch;

public class DbCreateFunction extends RootOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The connection to the database database. */
  private AccessMethod accessMethod;
  /** The information for the database connection. */
  private ConnectionInfo connectionInfo;
  /** function name.*/
  private final String name;
  /** function body.*/
  private final String binary;
  /** function language.*/
  private final MyriaConstants.FunctionLanguage lang;
  /** function output schema.*/
  private final Schema outputSchema;

  /** logger for this class. */
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(GenericEvaluator.class);

  /**
   * @param child the source of tuples to be inserted.
   * @param name function name.
   * @param connectionInfo the parameters of the database connection.
   * @param outputSchema output schema for the function
   * @param lang function type
   * @param binary function body (encoded binary string)
   * @param description function decription, this is kept in the catalog and not sent to workers.
   */
  public DbCreateFunction(
      final Operator child,
      final String name,
      final String description,
      final MyriaConstants.FunctionLanguage lang,
      final Schema outputSchema,
      final String binary,
      final ConnectionInfo connectionInfo) {
    super(child);
    this.name = name;
    this.connectionInfo = connectionInfo;
    this.lang = lang;
    this.binary = binary;
    this.outputSchema = outputSchema;
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars)
      throws DbException, IOException {
    /* Retrieve connection information from the environment variables, if not already set */
    if (connectionInfo == null && execEnvVars != null) {
      connectionInfo =
          (ConnectionInfo) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_DATABASE_CONN_INFO);
    }

    if (lang == MyriaConstants.FunctionLanguage.PYTHON) {
      //LOGGER.info("in python function register");

      BASE64Decoder decoder = new BASE64Decoder();
      if (binary != null) {
        byte[] decodedBytes = decoder.decodeBuffer(binary);
        ByteBuffer binaryFunction = ByteBuffer.wrap(decodedBytes);

        PythonFunctionRegistrar pyFunc = new PythonFunctionRegistrar(connectionInfo);
        pyFunc.addFunction(name, binaryFunction, outputSchema.toString());
      } else {
        throw new DbException("Cannot register python UDF without binary");
      }
    }
  }

  @Override
  public void cleanup() {
    try {
      if (accessMethod != null) {
        accessMethod.close();
      }
    } catch (DbException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void consumeTuples(final TupleBatch tuples) throws DbException {}

  @Override
  protected void childEOS() throws DbException {}

  @Override
  protected void childEOI() throws DbException {}
}
