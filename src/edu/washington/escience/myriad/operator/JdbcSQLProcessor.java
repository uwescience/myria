package edu.washington.escience.myriad.operator;

// import edu.washington.escience.Schema;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.parallel.DbException;
import edu.washington.escience.myriad.table._TupleBatch;

public class JdbcSQLProcessor extends JdbcQueryScan {

  private Operator child;

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public JdbcSQLProcessor(final String driverClass, final String connectionString, final String baseSQL, final Schema schema, final Operator child,
      final String username, final String password) {
    super(driverClass, connectionString, baseSQL, schema, username, password);
    this.child = child;
  }

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
  protected _TupleBatch fetchNext() throws DbException {
    return super.fetchNext();
  }

  @Override
  public Operator[] getChildren() {
    return new Operator[] { this.child };
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
  public void setChildren(final Operator[] children) {
    this.child = children[0];
  }

}
