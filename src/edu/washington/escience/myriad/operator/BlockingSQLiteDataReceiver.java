package edu.washington.escience.myriad.operator;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.MyriaConstants;
import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.accessmethod.SQLiteInfo;
import edu.washington.escience.myriad.util.SQLiteUtils;

/**
 * Blocking when receiving data from children.
 * */
public final class BlockingSQLiteDataReceiver extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /**
   * the child.
   * */
  private Operator child;
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
    this.child = child;
    this.relationKey = relationKey;
  }

  @Override
  protected void cleanup() throws DbException {
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    TupleBatch tb = null;
    tb = child.nextReady();
    while (tb != null) {
      SQLiteUtils.insertIntoSQLite(child.getSchema(), relationKey, sqliteInfo, tb);
      tb = child.nextReady();
    }
    return null;
  }

  @Override
  public Operator[] getChildren() {
    return new Operator[] { child };
  }

  @Override
  public Schema getSchema() {
    return child.getSchema();
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    sqliteInfo = (SQLiteInfo) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_DATABASE_CONN_INFO);
    if (sqliteInfo == null) {
      throw new DbException("Unable to instantiate SQLiteQueryScan on non-sqlite worker");
    }
  }

  @Override
  public void setChildren(final Operator[] children) {
    child = children[0];
  }
}
