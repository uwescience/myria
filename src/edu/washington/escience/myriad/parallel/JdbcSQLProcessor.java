package edu.washington.escience.myriad.parallel;

// import edu.washington.escience.Schema;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.table._TupleBatch;

public class JdbcSQLProcessor extends JdbcQueryScan {

  private Operator child;

  public JdbcSQLProcessor(String driverClass, String connectionString, String baseSQL, Schema schema, Operator child,
      String username, String password) {
    super(driverClass, connectionString, baseSQL, schema, username, password);
    this.child = child;
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
    this.child.close();
  }

  @Override
  public void open() throws DbException {
    this.child.open();
    while (child.hasNext()) {
      child.next();
    }
    super.open();
  }

  @Override
  protected _TupleBatch fetchNext() throws DbException {
    return super.fetchNext();
  }

  @Override
  public Operator[] getChildren() {
    return new Operator[] { this.child };
  }

  @Override
  public void setChildren(Operator[] children) {
    this.child = children[0];
  }

}
