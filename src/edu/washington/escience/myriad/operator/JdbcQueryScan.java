package edu.washington.escience.myriad.operator;

import java.util.Iterator;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.accessmethod.JdbcAccessMethod;

public class JdbcQueryScan extends LeafOperator {

  private Iterator<TupleBatch> tuples;
  private final Schema schema;
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
    schema = outputSchema;
  }

  @Override
  public void cleanup() {
    tuples = null;
  }

  @Override
  protected TupleBatch fetchNext() throws DbException {
    if (tuples.hasNext()) {
      final TupleBatch tb = tuples.next();
      return tb;
    } else {
      return null;
    }
  }

  @Override
  public void checkEOSAndEOI() {
    setEOS(true);
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public void init() throws DbException {
    tuples = JdbcAccessMethod.tupleBatchIteratorFromQuery(driverClass, connectionString, baseSQL, username, password);
  }

  @Override
  public TupleBatch fetchNextReady() throws DbException {
    return fetchNext();
  }

}
