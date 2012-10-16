package edu.washington.escience.myriad.operator;

import java.util.Iterator;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.accessmethod.SQLiteAccessMethod;
import edu.washington.escience.myriad.table._TupleBatch;

public class SQLiteQueryScan extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  private Iterator<TupleBatch> tuples;
  private final Schema schema;
  private final String filename;
  private final String baseSQL;
  private transient String dataDir;

  public SQLiteQueryScan(final String filename, final String baseSQL, final Schema outputSchema) {
    this.baseSQL = baseSQL;
    this.filename = filename;
    schema = outputSchema;
  }

  @Override
  public void close() {
    super.close();
    tuples = null;
  }

  @Override
  protected _TupleBatch fetchNext() throws DbException {

    if (tuples == null) {
      tuples = SQLiteAccessMethod.tupleBatchIteratorFromQuery(dataDir + "/" + filename, baseSQL, schema);
    }
    if (tuples.hasNext()) {
      return tuples.next();
    } else {
      return null;
      // }
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
  public void setChildren(final Operator[] children) {
    throw new UnsupportedOperationException();
  }

  public void setDataDir(final String dataDir) {
    this.dataDir = dataDir;
  }

}
