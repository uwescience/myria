package edu.washington.escience.parallel;

import java.util.Iterator;

import edu.washington.escience.JdbcAccessMethod;
import edu.washington.escience.Schema;
import edu.washington.escience.TupleBatch;

public class JdbcSeqScan extends Operator {

  /**
	 * 
	 */
  private static final long serialVersionUID = 1L;

  private Iterator<TupleBatch> tuples;
  private String driverClass;
  private String connectionString;
  private String baseSQL;
  private Schema schema;
  private TupleBatch cache;

  public JdbcSeqScan(String driverClass, String connectionString, String baseSQL) {
    this.driverClass = driverClass;
    this.connectionString = connectionString;
    this.baseSQL = baseSQL;

    tuples = JdbcAccessMethod.tupleBatchIteratorFromQuery(driverClass, connectionString, baseSQL);
    if (tuples.hasNext()) {
      cache = tuples.next();
      schema = cache.validSchema();
    } else {
      schema = null;
      cache = null;
    }
  }

  @Override
  public void close() {
    super.close();
    this.tuples = null;
  }

  @Override
  protected TupleBatch fetchNext() throws DbException {
    if (cache != null) {
      TupleBatch tmp = cache;
      cache = null;
      return tmp;
    } else {
      if (tuples.hasNext())
        return this.tuples.next();
      else
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
  public void open() throws DbException {

    super.open();
  }

  @Override
  public void rewind() throws DbException {
    tuples = JdbcAccessMethod.tupleBatchIteratorFromQuery(driverClass, connectionString, baseSQL);
    cache = null;

  }

  @Override
  public void setChildren(Operator[] children) {
    throw new UnsupportedOperationException();
  }

}
