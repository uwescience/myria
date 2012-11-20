package edu.washington.escience.myriad.operator;

// import edu.washington.escience.Schema;
import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;

public class JdbcSQLProcessor extends JdbcQueryScan {

  private Operator child;

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  public JdbcSQLProcessor(final String driverClass, final String connectionString, final String baseSQL,
      final Schema schema, final Operator child, final String username, final String password) {
    super(driverClass, connectionString, baseSQL, schema, username, password);
    this.child = child;
  }

  // @Override
  // public void rewind() throws DbException {
  // super.rewind();
  // child.rewind();
  // }

  @Override
  public Operator[] getChildren() {
    return new Operator[] { child };
  }

  @Override
  public void init() throws DbException {
    while (child.next() != null) {
    }
  }

  @Override
  public void setChildren(final Operator[] children) {
    child = children[0];
  }

}
