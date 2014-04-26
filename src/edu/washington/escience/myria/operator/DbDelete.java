package edu.washington.escience.myria.operator;

import java.io.File;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.accessmethod.AccessMethod;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.accessmethod.SQLiteInfo;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * An operator for deleting tuples from the underlying relations. The tuples passed into this operator are the tuples
 * that are being deleted from the database.
 */
public class DbDelete extends RootOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The connection to the database database. */
  private AccessMethod accessMethod;
  /** The relation that has the tuples. */
  private final RelationKey relationKey;
  /** The information for the database connection. */
  private ConnectionInfo connectionInfo;

  /**
   * Constructs a DbDelete object for deleting tuples from the relation specified by the relation key.
   * 
   * @param child the child to feed the tuples to this operator
   * @param relationKey the relation key of the relation
   */
  public DbDelete(final Operator child, final RelationKey relationKey) {
    this(child, relationKey, null);
  }

  /**
   * Constructs a DbDelete object for deleting tuples from the relation specified by the relation key.
   * 
   * @param child the child to feed the tuples to this operator
   * @param relationKey the relation key of the relation
   * @param connectionInfo the database connection
   */
  public DbDelete(final Operator child, final RelationKey relationKey, final ConnectionInfo connectionInfo) {
    super(child);
    Preconditions.checkNotNull(relationKey);
    this.relationKey = relationKey;
    this.connectionInfo = connectionInfo;
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    /* retrieve connection information from the environment variables, if not already set */
    if (connectionInfo == null && execEnvVars != null) {
      connectionInfo = (ConnectionInfo) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_DATABASE_CONN_INFO);
    }

    if (connectionInfo == null) {
      throw new DbException("Unable to instantiate DbDelete: connection information unknown");
    }

    if (connectionInfo instanceof SQLiteInfo) {
      /* Set WAL in the beginning. */
      final File dbFile = new File(((SQLiteInfo) connectionInfo).getDatabaseFilename());
      SQLiteConnection conn = new SQLiteConnection(dbFile);
      try {
        conn.open(true);
        conn.exec("PRAGMA journal_mode=WAL;");
      } catch (SQLiteException e) {
        e.printStackTrace();
      }
      conn.dispose();
    }

    /* open the database connection */
    accessMethod = AccessMethod.of(connectionInfo.getDbms(), connectionInfo, false);
  }

  /**
   * @return the name of the relation that this operator will write to.
   */
  public RelationKey getRelationKey() {
    return relationKey;
  }

  @Override
  protected void consumeTuples(final TupleBatch tuples) throws DbException {
    accessMethod.tupleBatchDelete(relationKey, tuples);
  }

  @Override
  protected void childEOS() throws DbException {
  }

  @Override
  protected void childEOI() throws DbException {
  }

}
