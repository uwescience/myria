package edu.washington.escience.myriad.operator;

// import edu.washington.escience.Schema;
import java.util.Iterator;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.accessmethod.JdbcAccessMethod;

public class JdbcSQLProcessor extends Operator {

  private Operator child;

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  private Iterator<TupleBatch> tuples;
  private final Schema schema;
  private final String driverClass;
  private final String connectionString;
  private final String baseSQL;
  private final String username;
  private final String password;

  public JdbcSQLProcessor(final String driverClass, final String connectionString, final String baseSQL,
      final Schema schema, final Operator child, final String username, final String password) {
    this.driverClass = driverClass;
    this.connectionString = connectionString;
    this.baseSQL = baseSQL;
    this.username = username;
    this.password = password;
    this.schema = schema;
    this.child = child;
  }

  @Override
  public Operator[] getChildren() {
    return new Operator[] { child };
  }

  private boolean checked = false;

  @Override
  protected TupleBatch fetchNext() throws DbException {
    if (!checked) {
      while (child.next() != null) {
      }
      checked = true;
    }
    if (tuples == null) {
      tuples = JdbcAccessMethod.tupleBatchIteratorFromQuery(driverClass, connectionString, baseSQL, username, password);
    }
    if (tuples.hasNext()) {
      return tuples.next();
    } else {
      return null;
    }
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public void init() throws DbException {
  }

  @Override
  public void setChildren(final Operator[] children) {
    child = children[0];
  }

  @Override
  public TupleBatch fetchNextReady() throws DbException {
    return fetchNext();
  }

  @Override
  public void cleanup() {
    tuples = null;
  }
}
