package edu.washington.escience.myriad.operator;

import java.util.Iterator;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.accessmethod.JdbcAccessMethod;
import edu.washington.escience.myriad.table._TupleBatch;

public class JdbcQueryScan extends Operator {

  private Iterator<TupleBatch> tuples;
  private final Schema schema;
  // private TupleBatch cache;
  private final String driverClass;
  private final String connectionString;
  private final String baseSQL;
  private final String username;
  private final String password;

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  public JdbcQueryScan(final String driverClass, final String connectionString, final String baseSQL,
      final Schema outputSchema, final String username, final String password) {
    this.driverClass = driverClass;
    this.connectionString = connectionString;
    this.baseSQL = baseSQL;
    this.username = username;
    this.password = password;
    // if (tuples.hasNext()) {
    // cache = tuples.next();
    schema = outputSchema;
    // } else {
    // schema = null;
    // cache = null;
    // }
  }

  @Override
  public void cleanup() {
    tuples = null;
  }

  @Override
  protected _TupleBatch fetchNext() throws DbException {
    if (tuples.hasNext()) {
      return tuples.next();
    } else {
      return null;
    }
  }

  @Override
  public Operator[] getChildren() {
    return null;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public void init() throws DbException {
    tuples = JdbcAccessMethod.tupleBatchIteratorFromQuery(driverClass, connectionString, baseSQL, username, password);
  }

  // @Override
  // public void rewind() throws DbException {
  // tuples = JdbcAccessMethod.tupleBatchIteratorFromQuery(driverClass, connectionString, baseSQL);
  // cache = null;
  // }

  @Override
  public void setChildren(final Operator[] children) {
    throw new UnsupportedOperationException();
  }

  @Override
  public _TupleBatch fetchNextReady() throws DbException {
    return fetchNext();
  }

}
