/**
 *
 */
package edu.washington.escience.myria.operator;

import java.util.List;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.accessmethod.AccessMethod;
import edu.washington.escience.myria.accessmethod.AccessMethod.IndexRef;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * 
 */
public class DbCreateIndex extends RootOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The connection to the database database. */
  private AccessMethod accessMethod;
  /** The information for the database connection. */
  private ConnectionInfo connectionInfo;

  /** The name of the table the tuples should be inserted into. */
  private final RelationKey relationKey;
  private final Schema schema;
  private final List<IndexRef> indexes;

  public DbCreateIndex(final Operator child, final RelationKey relationKey, final Schema schema,
      final List<IndexRef> indexes, final ConnectionInfo connectionInfo) {
    super(child);
    this.connectionInfo = connectionInfo;
    this.relationKey = relationKey;
    this.schema = schema;
    this.indexes = indexes;
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    /* Retrieve connection information from the environment variables, if not already set */
    if (connectionInfo == null && execEnvVars != null) {
      connectionInfo = (ConnectionInfo) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_DATABASE_CONN_INFO);
    }
    /* Open the database connection */
    accessMethod = AccessMethod.of(connectionInfo.getDbms(), connectionInfo, false);

    /* Add the indexes to the relation */
    accessMethod.createIndexIfNotExists(relationKey, schema, indexes);
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
