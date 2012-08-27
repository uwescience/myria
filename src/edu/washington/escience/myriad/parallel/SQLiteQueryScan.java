package edu.washington.escience.myriad.parallel;

import java.util.Iterator;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.accessmethod.SQLiteAccessMethod;

public class SQLiteQueryScan extends Operator {

  /**
   * 
   */
  private Iterator<TupleBatch> tuples;
  private Schema schema;
  private TupleBatch cache;

  public SQLiteQueryScan(String pathToFile, String baseSQL) {
    tuples = SQLiteAccessMethod.tupleBatchIteratorFromQuery(pathToFile, baseSQL);
    if (tuples.hasNext()) {
      cache = tuples.next();
      schema = cache.getSchema();
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

  // @Override
  // public void rewind() throws DbException {
  // tuples = SQLiteAccessMethod.tupleBatchIteratorFromQuery(driverClass, baseSQL);
  // cache = null;
  // }

  @Override
  public void setChildren(Operator[] children) {
    throw new UnsupportedOperationException();
  }

}
