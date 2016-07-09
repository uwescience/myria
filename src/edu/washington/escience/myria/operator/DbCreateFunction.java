/**
 *
 */
package edu.washington.escience.myria.operator;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.accessmethod.AccessMethod;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 *
 */
public class DbCreateFunction extends RootOperator {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The connection to the database database. */
  private AccessMethod accessMethod;
  /** The information for the database connection. */
  private ConnectionInfo connectionInfo;

  private final String functionName;
  private final String functionDefinition;

  /**
   * @param child the source of tuples to be inserted.
   * @param relationKey the key of the table the tuples should be inserted into.
   * @param connectionInfo the parameters of the database connection.
   */
  public DbCreateFunction(
      final Operator child,
      final String functionName,
      final String functionDefinition,
      final ConnectionInfo connectionInfo) {
    super(child);
    this.connectionInfo = connectionInfo;
    this.functionName = functionName;
    this.functionDefinition = functionDefinition;
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    /* Retrieve connection information from the environment variables, if not already set */
    if (connectionInfo == null && execEnvVars != null) {
      connectionInfo =
          (ConnectionInfo) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_DATABASE_CONN_INFO);
    }
    /* Open the database connection */
    accessMethod = AccessMethod.of(connectionInfo.getDbms(), connectionInfo, false);

    /* Validate command */
    Pattern pattern = Pattern.compile("(CREATE FUNCTION)([\\s\\S]*)(LANGUAGE SQL;)");
    Matcher matcher = pattern.matcher(functionDefinition);

    if (matcher.matches()) {
      /* Run command */
      accessMethod.runCommand(functionDefinition);
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
