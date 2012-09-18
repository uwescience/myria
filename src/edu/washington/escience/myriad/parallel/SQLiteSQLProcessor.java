package edu.washington.escience.myriad.parallel;

// import edu.washington.escience.Schema;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.table._TupleBatch;

public class SQLiteSQLProcessor extends SQLiteQueryScan {

  private Operator[] children;

  public SQLiteSQLProcessor(String filename, String baseSQL, Schema schema, Operator[] children) {
    super(filename, baseSQL, schema);
    this.children = children;
  }

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  // @Override
  // public void rewind() throws DbException {
  // super.rewind();
  // child.rewind();
  // }

  @Override
  public void close() {
    super.close();
    for (Operator child : children)
      child.close();
  }

  @Override
  public void open() throws DbException {
    for (Operator child : children) {
      child.open();
    }
    
    for (Operator child : children) {
      while (child.hasNext()) {
        child.next();
      }
    }
    super.open();
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
  public void setChildren(Operator[] children) {
    this.children = children;
  }

}
