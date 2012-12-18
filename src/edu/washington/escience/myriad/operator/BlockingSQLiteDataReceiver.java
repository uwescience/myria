package edu.washington.escience.myriad.operator;

import edu.washington.escience.myriad.DbException;
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
  final String dbFileName;
  final String tableName;
  String dataDir = ".";

  public BlockingSQLiteDataReceiver(final String dbFileName, final String tableName, final Operator child) {
    this.child = child;
    this.dbFileName = dbFileName;
    this.tableName = tableName;
  }

  @Override
  protected TupleBatch fetchNext() throws DbException {
    TupleBatch tb = null;
    while ((tb = child.next()) != null) {
      SQLiteUtils.insertIntoSQLite(child.getSchema(), tableName, dataDir + "/" + dbFileName, tb);
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
  public void init() throws DbException {
  }

  @Override
  public void setChildren(final Operator[] children) {
    child = children[0];
  }

  @Override
  protected void cleanup() throws DbException {
  }

  @Override
  public TupleBatch fetchNextReady() throws DbException {
    return fetchNext();
  }

  public void resetDataDir(String dataDir) {
    this.dataDir = dataDir;
  }
}
