package edu.washington.escience.myriad.parallel;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.table._TupleBatch;

public class SQLiteSQLProcessor extends SQLiteQueryScan {

  private Operator[] children;

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public SQLiteSQLProcessor(final String filename, final String baseSQL, final Schema schema, final Operator[] children) {
    super(filename, baseSQL, schema);
    this.children = children;
  }

  // @Override
  // public void rewind() throws DbException {
  // super.rewind();
  // child.rewind();
  // }

  @Override
  public void close() {
    super.close();
    for (final Operator child : children) {
      child.close();
    }
  }

  @Override
  protected _TupleBatch fetchNext() throws DbException {
    return super.fetchNext();
  }

  @Override
  public Operator[] getChildren() {
    return this.children;
  }

  @Override
  public void open() throws DbException {
    for (final Operator child : children) {
      child.open();
    }

    for (final Operator child : children) {
      while (child.hasNext()) {
        child.next();
      }
    }
    super.open();
  }

  @Override
  public void setChildren(final Operator[] children) {
    this.children = children;
  }

}
