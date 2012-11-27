package edu.washington.escience.myriad.operator;

import java.util.Iterator;
import java.util.Objects;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.accessmethod.SQLiteAccessMethod;
import edu.washington.escience.myriad.table._TupleBatch;

public class SQLiteQueryScan extends Operator {

  private static Operator[] children = new Operator[] {};

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  private Iterator<TupleBatch> tuples;
  private final Schema schema;
  private final String baseSQL;
  private String databaseFilename;

  /**
   * Construct a new SQLiteQueryScan object.
   * 
   * @param databaseFilename the full path to the SQLite database storing the data.
   * @param baseSQL the selection query.
   * @param outputSchema the Schema of the returned tuples.
   */
  public SQLiteQueryScan(final String databaseFilename, final String baseSQL, final Schema outputSchema) {
    Objects.requireNonNull(baseSQL);
    Objects.requireNonNull(outputSchema);
    this.baseSQL = baseSQL;
    schema = outputSchema;
    this.databaseFilename = databaseFilename;
  }

  @Override
  public void cleanup() {
    tuples = null;
  }

  @Override
  protected _TupleBatch fetchNext() throws DbException {
    if (tuples == null) {
      tuples = SQLiteAccessMethod.tupleBatchIteratorFromQuery(databaseFilename, baseSQL, schema);
    }
    if (tuples.hasNext()) {
      return tuples.next();
    } else {
      return null;
    }
  }

  @Override
  public Operator[] getChildren() {
    return children;
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
    throw new UnsupportedOperationException();
  }

  public void setPathToSQLiteDb(final String databaseFilename) throws DbException {
    if (isOpen()) {
      throw new DbException("Can't change the state of an opened operator.");
    }
    this.databaseFilename = databaseFilename;
  }

  @Override
  public _TupleBatch fetchNextReady() throws DbException {
    return fetchNext();
  }

}
