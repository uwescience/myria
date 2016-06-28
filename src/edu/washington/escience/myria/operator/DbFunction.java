/**
 *
 */
package edu.washington.escience.myria.operator;

import java.io.IOException;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.accessmethod.AccessMethod;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.expression.evaluate.GenericEvaluator;
import edu.washington.escience.myria.functions.PythonFunctionRegistrar;
import edu.washington.escience.myria.storage.TupleBatch;

;

/**
 * 
 */
public class DbFunction extends RootOperator {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The connection to the database database. */
  private AccessMethod accessMethod;
  /** The information for the database connection. */
  private ConnectionInfo connectionInfo;

  private final String name;
  private final String binary;

  private final MyriaConstants.FunctionLanguage lang;
  private final String text;
  /** logger for this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(GenericEvaluator.class);

  /**
   * @param child the source of tuples to be inserted.
   * @param relationKey the key of the table the tuples should be inserted into.
   * @param connectionInfo the parameters of the database connection.
   */
  public DbFunction(final Operator child, final String name, final String text,
      final MyriaConstants.FunctionLanguage lang, final String binary, final ConnectionInfo connectionInfo) {
    super(child);
    this.name = name;
    this.connectionInfo = connectionInfo;
    this.text = text;
    this.lang = lang;
    this.binary = binary;

  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException, IOException {
    /* Retrieve connection information from the environment variables, if not already set */
    if (connectionInfo == null && execEnvVars != null) {
      connectionInfo = (ConnectionInfo) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_DATABASE_CONN_INFO);
    }

    if (lang == MyriaConstants.FunctionLanguage.POSTGRES) {
      /* Open the database connection */
      accessMethod = AccessMethod.of(connectionInfo.getDbms(), connectionInfo, false);
      /* Add the POSTGRES UDF */
      accessMethod.executeSQLCommand(text);
    }
    if (lang == MyriaConstants.FunctionLanguage.PYTHON) {

      if (binary != null) {
        PythonFunctionRegistrar pyFunc = new PythonFunctionRegistrar(connectionInfo);
        pyFunc.addUDF(name, binary);
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
  protected void consumeTuples(final TupleBatch tuples) throws DbException {
  }

  @Override
  protected void childEOS() throws DbException {
  }

  @Override
  protected void childEOI() throws DbException {
  }

}
