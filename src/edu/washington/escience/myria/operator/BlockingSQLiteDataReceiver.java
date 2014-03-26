package edu.washington.escience.myria.operator;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.accessmethod.SQLiteInfo;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.SQLiteUtils;

/**
 * Blocking when receiving data from children.
 * */
public final class BlockingSQLiteDataReceiver extends UnaryOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /**
   * SQLite connection info.
   * */
  private SQLiteInfo sqliteInfo;

  /**
   * the relation for querying data from.
   * */
  private final RelationKey relationKey;

  /**
   * @param relationKey the source relation.
   * @param child the child.
   * */
  public BlockingSQLiteDataReceiver(final RelationKey relationKey, final Operator child) {
    super(child);
    this.relationKey = relationKey;
  }

  @Override
  protected void cleanup() throws DbException {
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    final Operator child = getChild();
    TupleBatch tb = null;
    tb = child.nextReady();
    while (tb != null) {
      SQLiteUtils.insertIntoSQLite(child.getSchema(), relationKey, sqliteInfo, tb);
      tb = child.nextReady();
    }
    return null;
  }

  @Override
  protected Schema generateSchema() {
    Operator child = getChild();
    if (child == null) {
      return null;
    }
    return child.getSchema();
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    sqliteInfo = (SQLiteInfo) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_DATABASE_CONN_INFO);
    if (sqliteInfo == null) {
      throw new DbException("Unable to instantiate SQLiteQueryScan on non-sqlite worker");
    }
  }
}
