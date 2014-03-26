package edu.washington.escience.myria.operator;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.accessmethod.JdbcAccessMethod;
import edu.washington.escience.myria.accessmethod.JdbcInfo;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Blocking when receiving data from children.
 * */
public final class BlockingJDBCDataReceiver extends UnaryOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * JDBC info.
   * */
  private final JdbcInfo jdbcInfo;

  /**
   * The relation to insert into.
   */
  private final RelationKey relationKey;

  /**
   * @param relationKey the table to insert into.
   * @param jdbcInfo the JDBC info.
   * @param child the child.
   * */
  public BlockingJDBCDataReceiver(final RelationKey relationKey, final JdbcInfo jdbcInfo, final Operator child) {
    super(child);
    this.jdbcInfo = jdbcInfo;
    this.relationKey = relationKey;
  }

  @Override
  protected void cleanup() throws DbException {
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    final Operator child = getChild();
    TupleBatch tb = null;
    tb = child.nextReady();
    while (tb != null) {
      JdbcAccessMethod.tupleBatchInsert(jdbcInfo, relationKey, getSchema(), tb);
      tb = child.nextReady();
    }
    return null;
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
  }

  @Override
  protected Schema generateSchema() {
    Operator child = getChild();
    if (child == null) {
      return null;
    }
    return child.getSchema();
  }

}
