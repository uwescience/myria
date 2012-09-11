package edu.washington.escience.myriad.parallel;

import java.util.Iterator;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.table._TupleBatch;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.accessmethod.JdbcAccessMethod;
import edu.washington.escience.myriad.accessmethod.SQLiteAccessMethod;

public class SQLiteQueryScan extends Operator {

  private Iterator<TupleBatch> tuples;
  private final Schema schema;
  // private TupleBatch cache;
  private final String filePath;
  private final String baseSQL;

  public SQLiteQueryScan(String filePath, String baseSQL, Schema outputSchema) {
    this.baseSQL = baseSQL;
    this.filePath = filePath;
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
    tuples = SQLiteAccessMethod.tupleBatchIteratorFromQuery(filePath, baseSQL);
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
