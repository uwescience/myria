package edu.washington.escience.myriad.operator;

import java.util.Iterator;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.accessmethod.JdbcAccessMethod;

public class JdbcQueryScan extends LeafOperator {

  private transient Iterator<TupleBatch> tuples;
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
  protected TupleBatch fetchNext() throws DbException, InterruptedException {
    if (tuples.hasNext()) {
      final TupleBatch tb = tuples.next();
      return tb;
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
    tuples = JdbcAccessMethod.tupleBatchIteratorFromQuery(driverClass, connectionString, baseSQL, username, password);
  }

}
