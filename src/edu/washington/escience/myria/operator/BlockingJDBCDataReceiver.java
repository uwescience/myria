package edu.washington.escience.myria.operator;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.accessmethod.JdbcAccessMethod;
import edu.washington.escience.myria.accessmethod.JdbcInfo;

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
   * the source table name.
   * */
  private final String tableName;

  /**
   * The columns the query should get from the JDBC database.
   * */
  private final List<String> columnNames;

  /**
   * Question mark place holders in the query SQL statement.
   * */
  private final String[] placeHolders;

  /**
   * @param tableName the table name.
   * @param jdbcInfo the JDBC info.
   * @param child the child.
   * */
  public BlockingJDBCDataReceiver(final String tableName, final JdbcInfo jdbcInfo, final Operator child) {
    super(child);
    this.tableName = tableName;
    this.jdbcInfo = jdbcInfo;
    final Schema s = child.getSchema();
    columnNames = s.getColumnNames();
    placeHolders = new String[s.numColumns()];
    for (int i = 0; i < s.numColumns(); ++i) {
      placeHolders[i] = "?";
    }
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
      JdbcAccessMethod.tupleBatchInsert(jdbcInfo, "insert into " + tableName + " ( "
          + StringUtils.join(columnNames, ',') + " ) values ( " + StringUtils.join(placeHolders, ',') + " )", tb);
      tb = child.nextReady();
    }
    return null;
  }

  @Override
  public Schema getSchema() {
    return getChild().getSchema();
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
  }

}
