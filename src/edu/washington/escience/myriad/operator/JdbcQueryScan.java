package edu.washington.escience.myriad.operator;

import java.util.Iterator;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.accessmethod.JdbcAccessMethod;
import edu.washington.escience.myriad.accessmethod.JdbcInfo;

/**
 * Push a select query down into a JDBC based database and scan over the query result.
 * */
public class JdbcQueryScan extends LeafOperator {

  /**
   * Iterate over data from the JDBC database.
   * */
  private transient Iterator<TupleBatch> tuples;
  /**
   * The result schema.
   * */
  private final Schema outputSchema;

  /** The information for the JDBC connection. */
  private final JdbcInfo jdbcInfo;
  /**
   * The SQL template.
   * */
  private final String baseSQL;

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * @param jdbcInfo see the corresponding field.
   * @param baseSQL see the corresponding field.
   * @param outputSchema see the corresponding field.
   * */
  public JdbcQueryScan(final JdbcInfo jdbcInfo, final String baseSQL, final Schema outputSchema) {
    this.jdbcInfo = jdbcInfo;
    this.baseSQL = baseSQL;
    this.outputSchema = outputSchema;
  }

  @Override
  public final void cleanup() {
    tuples = null;
  }

  @Override
  protected final TupleBatch fetchNext() throws DbException, InterruptedException {
    if (tuples.hasNext()) {
      final TupleBatch tb = tuples.next();
      return tb;
    } else {
      return null;
    }
  }

  @Override
  protected final TupleBatch fetchNextReady() throws DbException {
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
  public final Schema getSchema() {
    return outputSchema;
  }

  @Override
  protected final void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    tuples = JdbcAccessMethod.tupleBatchIteratorFromQuery(jdbcInfo, baseSQL);
  }

}
