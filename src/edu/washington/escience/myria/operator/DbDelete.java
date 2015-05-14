/**
 *
 */
package edu.washington.escience.myria.operator;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.accessmethod.AccessMethod;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * 
 * TODO: it's not really an "operator", is it?
 */
public class DbDelete extends RootOperator {

  /** The connection to the database database. */
  private AccessMethod accessMethod;
  /** The information for the database connection. */
  private ConnectionInfo connectionInfo;
  /** The name of the table the tuples should be inserted into. */
  private final RelationKey relationKey;

  /**
   * @param child the source of tuples to be inserted.
   * @param relationKey the key of the table the tuples should be inserted into.
   * @param connectionInfo the parameters of the database connection.
   */
  public DbDelete(final Operator child, final RelationKey relationKey, final ConnectionInfo connectionInfo) {
    super(child);
    this.connectionInfo = connectionInfo;
    this.relationKey = relationKey;
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    /* retrieve connection information from the environment variables, if not already set */
    if (connectionInfo == null && execEnvVars != null) {
      connectionInfo = (ConnectionInfo) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_DATABASE_CONN_INFO);
    }
    /* open the database connection */
    accessMethod = AccessMethod.of(connectionInfo.getDbms(), connectionInfo, false);

    /* Drop the table */
    accessMethod.dropTableIfExistsCascade(relationKey);
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
