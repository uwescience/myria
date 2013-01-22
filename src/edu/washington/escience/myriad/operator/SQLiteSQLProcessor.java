package edu.washington.escience.myriad.operator;

import java.util.Iterator;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.accessmethod.SQLiteAccessMethod;

public class SQLiteSQLProcessor extends Operator {

  private Operator[] children;

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  private Iterator<TupleBatch> tuples;
  private final Schema schema;
  private final String baseSQL;
  private String databaseFilename;

  public SQLiteSQLProcessor(final String databaseFilename, final String baseSQL, final Schema schema,
      final Operator[] children) {
    this.baseSQL = baseSQL;
    this.schema = schema;
    this.children = children;
    this.databaseFilename = databaseFilename;
  }

  @Override
  public void cleanup() {
    tuples = null;
  }

  private boolean checked = false;

  @Override
  protected TupleBatch fetchNext() throws DbException {
    if (!checked) {
      for (final Operator child : children) {
        while (child.next() != null) {
        }
      }
      checked = true;
    }
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
    this.children = children;
  }

  @Override
  public TupleBatch fetchNextReady() throws DbException {
    return fetchNext();
  }

  public void setPathToSQLiteDb(final String databaseFilename) throws DbException {
    if (isOpen()) {
      throw new DbException("Can't change the state of an opened operator.");
    }
    this.databaseFilename = databaseFilename;
  }
}
