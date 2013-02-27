package edu.washington.escience.myriad.operator;

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

  private Operator child;
  String pathToSQLiteDb;
  final RelationKey relationKey;

  public BlockingSQLiteDataReceiver(final String pathToSQLiteDb, final RelationKey relationKey, final Operator child) {
    this.child = child;
    this.pathToSQLiteDb = pathToSQLiteDb;
    this.relationKey = relationKey;
  }

  @Override
  protected void cleanup() throws DbException {
  }

  @Override
  protected TupleBatch fetchNext() throws DbException {
    TupleBatch tb = null;
    while (!child.eos()) {
      while ((tb = child.next()) != null) {
        SQLiteUtils.insertIntoSQLite(child.getSchema(), relationKey, pathToSQLiteDb, tb);
      }
      if (child.eoi()) {
        child.setEOI(false);
      }
    }
    return null;
  }

  @Override
  public TupleBatch fetchNextReady() throws DbException {
    return fetchNext();
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
  public void init() throws DbException {
  }

  @Override
  public void setChildren(final Operator[] children) {
    child = children[0];
  }

  public void setPathToSQLiteDb(final String pathToSQLiteDb) {
    this.pathToSQLiteDb = pathToSQLiteDb;
  }
}
