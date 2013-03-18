package edu.washington.escience.myriad.operator;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.accessmethod.JdbcAccessMethod;
import edu.washington.escience.myriad.accessmethod.JdbcInfo;

/**
 * Blocking when receiving data from children.
 * */
public final class BlockingJDBCDataReceiver extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  private Operator child;
  private final JdbcInfo jdbcInfo;
  private final String tableName;
  private final List<String> fieldNames;
  private final String[] placeHolders;

  public BlockingJDBCDataReceiver(final String tableName, final JdbcInfo jdbcInfo, final Operator child) {
    this.tableName = tableName;
    this.child = child;
    this.jdbcInfo = jdbcInfo;
    final Schema s = child.getSchema();
    fieldNames = s.getColumnNames();
    placeHolders = new String[s.numColumns()];
    for (int i = 0; i < s.numColumns(); ++i) {
      placeHolders[i] = "?";
    }
  }

  @Override
  protected void cleanup() throws DbException {
  }

  @Override
  protected TupleBatch fetchNext() throws DbException, InterruptedException {
    TupleBatch tb = null;
    while (!child.eos()) {
      while ((tb = child.next()) != null) {
        JdbcAccessMethod.tupleBatchInsert(jdbcInfo, "insert into " + tableName + " ( "
            + StringUtils.join(fieldNames, ',') + " ) values ( " + StringUtils.join(placeHolders, ',') + " )", tb);
      }
    }
    return null;
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    TupleBatch tb = null;
    while (!child.eos() && (tb = child.nextReady()) != null) {
      // tb = child.next();
      JdbcAccessMethod.tupleBatchInsert(driverClass, connectionString, "insert into " + tableName + " ( "
          + StringUtils.join(fieldNames, ',') + " ) values ( " + StringUtils.join(placeHolders, ',') + " )", tb,
          username, password);
    }
    return null;
  }

  @Override
  public Operator[] getChildren() {
    return new Operator[] { child };
  }

  @Override
  public Schema getSchema() {
    return child.getSchema();
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
  }

  @Override
  public void setChildren(final Operator[] children) {
    child = children[0];
  }

}
