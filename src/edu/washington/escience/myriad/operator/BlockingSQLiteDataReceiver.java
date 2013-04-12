package edu.washington.escience.myriad.operator;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
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
   * Path to Sqlite db file.
   * */
  private String pathToSQLiteDb;

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
  protected TupleBatch fetchNext() throws DbException, InterruptedException {
    TupleBatch tb = null;
    while (!child.eos()) {
      while ((tb = child.next()) != null) {
        SQLiteUtils.insertIntoSQLite(child.getSchema(), relationKey, pathToSQLiteDb, tb);
      }
    }
    return null;
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    TupleBatch tb = null;
    tb = child.nextReady();
    while (tb != null) {
      SQLiteUtils.insertIntoSQLite(child.getSchema(), relationKey, pathToSQLiteDb, tb);
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
    final String sqliteDatabaseFilename = (String) execEnvVars.get("sqliteFile");
    if (sqliteDatabaseFilename == null) {
      throw new DbException("Unable to instantiate SQLiteQueryScan on non-sqlite worker");
    }
    pathToSQLiteDb = sqliteDatabaseFilename;
  }

  @Override
  public void setChildren(final Operator[] children) {
    child = children[0];
  }
}
