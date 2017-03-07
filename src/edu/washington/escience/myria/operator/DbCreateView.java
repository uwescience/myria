/**
 *
 */
package edu.washington.escience.myria.operator;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.accessmethod.AccessMethod;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 *
 */
public class DbCreateView extends RootOperator {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The connection to the database database. */
  private AccessMethod accessMethod;
  /** The information for the database connection. */
  private ConnectionInfo connectionInfo;

  private final String viewName;
  private final String viewQuery;
  private final boolean isMaterialized;

  public DbCreateView(
      final Operator child,
      final String viewName,
      final String viewQuery,
      final boolean isMaterialized,
      final ConnectionInfo connectionInfo) {
    super(child);
    this.connectionInfo = connectionInfo;
    this.viewName = viewName;
    this.viewQuery = viewQuery;
    this.isMaterialized = isMaterialized;
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

    /* Add the indexes to the relation */
    if (isMaterialized) {
      accessMethod.createMaterializedView(viewName, viewQuery);
    } else {
      accessMethod.createView(viewName, viewQuery);
    }
  }

  @Override
  protected void consumeTuples(final TupleBatch tuples) throws DbException {}

  @Override
  protected void childEOS() throws DbException {}

  @Override
  protected void childEOI() throws DbException {}
}
