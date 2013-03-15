package edu.washington.escience.myriad.operator;

import java.util.Iterator;
import java.util.Objects;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.accessmethod.SQLiteAccessMethod;

public class SQLiteQueryScan extends LeafOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  private transient Iterator<TupleBatch> tuples;
  private final Schema schema;
  private final String baseSQL;
  private transient String databaseFilename;

  /**
   * Construct a new SQLiteQueryScan object.
   * 
   * @param baseSQL the selection query.
   * @param outputSchema the Schema of the returned tuples.
   */
  public SQLiteQueryScan(final String baseSQL, final Schema outputSchema) {
    Objects.requireNonNull(baseSQL);
    Objects.requireNonNull(outputSchema);
    this.baseSQL = baseSQL;
    schema = outputSchema;
  }

  @Override
  public void cleanup() {
    tuples = null;
  }

  @Override
  protected TupleBatch fetchNext() throws DbException, InterruptedException {
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
  protected TupleBatch fetchNextReady() throws DbException {
    try {
      TupleBatch tb = fetchNext();
      if (tb == null) {
        setEOS();
      }
      return tb;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    }
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    final String sqliteDatabaseFilename = (String) execEnvVars.get("sqliteFile");
    if (sqliteDatabaseFilename == null) {
      throw new DbException("Unable to instantiate SQLiteQueryScan on non-sqlite worker");
    }
    databaseFilename = sqliteDatabaseFilename;
    tuples = null;
  }

  @Override
  public String toString() {
    return "SQLiteQueryScan, baseSQL: " + baseSQL;
  }
}
