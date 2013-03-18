package edu.washington.escience.myriad.operator;

import java.util.Iterator;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.accessmethod.JdbcAccessMethod;
import edu.washington.escience.myriad.accessmethod.JdbcInfo;

public class JdbcQueryScan extends LeafOperator {

  private transient Iterator<TupleBatch> tuples;
  private final Schema schema;
  private final JdbcInfo jdbcInfo;
  private final String baseSQL;

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  public JdbcQueryScan(final JdbcInfo jdbcInfo, final String baseSQL, final Schema outputSchema) {
    this.jdbcInfo = jdbcInfo;
    this.baseSQL = baseSQL;
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
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    tuples = JdbcAccessMethod.tupleBatchIteratorFromQuery(jdbcInfo, baseSQL);
  }

}
