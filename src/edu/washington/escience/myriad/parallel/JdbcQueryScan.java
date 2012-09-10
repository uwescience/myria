package edu.washington.escience.myriad.parallel;

import java.util.Iterator;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.table._TupleBatch;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.accessmethod.JdbcAccessMethod;

public class JdbcQueryScan extends Operator {

  private Iterator<TupleBatch> tuples;
  private final Schema schema;
  // private TupleBatch cache;
  private final String driverClass;
  private final String connectionString;
  private final String baseSQL;
  private final String username;
  private final String password;

  public JdbcQueryScan(String driverClass, String connectionString, String baseSQL, Schema outputSchema,
      String username, String password) {
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
  public void close() {
    super.close();
    this.tuples = null;
  }

  @Override
  protected _TupleBatch fetchNext() throws DbException {
    // if (cache != null) {
    // TupleBatch tmp = cache;
    // cache = null;
    // return tmp;
    // } else {
    if (tuples.hasNext())
      return this.tuples.next();
    else
      return null;
    // }
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
  public void open() throws DbException {
    super.open();
    tuples = JdbcAccessMethod.tupleBatchIteratorFromQuery(driverClass, connectionString, baseSQL, username, password);
  }

  // @Override
  // public void rewind() throws DbException {
  // tuples = JdbcAccessMethod.tupleBatchIteratorFromQuery(driverClass, connectionString, baseSQL);
  // cache = null;
  // }

  @Override
  public void setChildren(Operator[] children) {
    throw new UnsupportedOperationException();
  }

}
