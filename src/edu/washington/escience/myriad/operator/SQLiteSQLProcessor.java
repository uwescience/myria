package edu.washington.escience.myriad.operator;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;

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
  public void cleanup() {
  }

  @Override
  public Operator[] getChildren() {
    return children;
  }

  @Override
  public void init() throws DbException {
    for (final Operator child : children) {
      while (child.next() != null) {
      }
    }
  }

  @Override
  public void setChildren(final Operator[] children) {
    this.children = children;
  }

}
