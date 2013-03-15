package edu.washington.escience.myriad.operator;

// import edu.washington.escience.Schema;
import java.util.Iterator;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.accessmethod.JdbcAccessMethod;
import edu.washington.escience.myriad.accessmethod.JdbcInfo;

public class JdbcSQLProcessor extends Operator {

  private Operator child;

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  private Iterator<TupleBatch> tuples;
  private final Schema schema;
  private final JdbcInfo jdbcInfo;
  private final String baseSQL;

  public JdbcSQLProcessor(final JdbcInfo jdbcInfo, final String baseSQL, final Schema schema, final Operator child) {
    this.jdbcInfo = jdbcInfo;
    this.baseSQL = baseSQL;
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
        assert true; /* Do nothing. */
      }
      checked = true;
    }
    if (tuples == null) {
      tuples = JdbcAccessMethod.tupleBatchIteratorFromQuery(jdbcInfo, baseSQL);
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
