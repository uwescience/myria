/**
 *
 */
package edu.washington.escience.myria.operator;

import java.nio.ByteBuffer;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.accessmethod.AccessMethod;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.api.encoding.FunctionEncoding.FunctionLanguage;
import edu.washington.escience.myria.storage.TupleBatch;

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

  private final ByteBuffer binary;
  private final Schema inputSchema;
  private final Schema outputSchema;
  private final FunctionLanguage lang;
  private final String text;

  /**
   * @param child the source of tuples to be inserted.
   * @param relationKey the key of the table the tuples should be inserted into.
   * @param connectionInfo the parameters of the database connection.
   */
  public DbFunction(final Operator child, final String text, final Schema input, final Schema output,
      final ByteBuffer binary, final FunctionLanguage lang, final ConnectionInfo connectionInfo) {
    super(child);
    this.connectionInfo = connectionInfo;
    this.text = text;
    outputSchema = output;
    inputSchema = input;
    this.lang = lang;
    this.binary = binary;

  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    /* Retrieve connection information from the environment variables, if not already set */
    if (connectionInfo == null && execEnvVars != null) {
      connectionInfo = (ConnectionInfo) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_DATABASE_CONN_INFO);
    }

    if (lang == FunctionLanguage.POSTGRES) {
      /* Open the database connection */
      accessMethod = AccessMethod.of(connectionInfo.getDbms(), connectionInfo, false);
      /* Add the POSTGRES UDF */
      accessMethod.executeSQLCommand(text);
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
