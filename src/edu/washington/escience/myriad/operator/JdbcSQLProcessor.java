package edu.washington.escience.myriad.operator;

import java.util.Iterator;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.accessmethod.JdbcAccessMethod;
import edu.washington.escience.myriad.accessmethod.JdbcInfo;

public class JdbcSQLProcessor extends Operator {

  private Operator[] children;
  private boolean allChildrenEOS = false;

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  private Iterator<TupleBatch> tuples;
  private final Schema schema;
  private final JdbcInfo jdbcInfo;
  private final String baseSQL;

  public JdbcSQLProcessor(final JdbcInfo jdbcInfo, final String baseSQL, final Schema schema, final Operator[] children) {
    this.jdbcInfo = jdbcInfo;
    this.baseSQL = baseSQL;
    this.schema = schema;
    this.children = children;
  }

  @Override
  public Operator[] getChildren() {
    return children;
  }

  private void waitChildren() throws DbException, InterruptedException {
    if (allChildrenEOS) {
      return;
    }
    for (final Operator child : children) {
      while (!child.eos()) {
        child.next();
      }
    }
    allChildrenEOS = true;
  }

  private void waitChildrenReady() throws DbException {
    for (final Operator child : children) {
      TupleBatch tb = null;
      while (!child.eos() && (tb = child.nextReady()) != null) {
      }
    }
    boolean tmpAllChildrenEOS = true;
    for (Operator child : children) {
      if (!child.eos()) {
        tmpAllChildrenEOS = false;
        break;
      }
    }
    allChildrenEOS = tmpAllChildrenEOS;

  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    try {
      if (allChildrenEOS) {
        return fetchNext();
      } else {
        waitChildrenReady();
        if (allChildrenEOS) {
          return fetchNext();
        }
      }
      return null;
    } catch (InterruptedException ee) {
      Thread.currentThread().interrupt();
      return null;
    }
  }

  @Override
  protected TupleBatch fetchNext() throws DbException, InterruptedException {
    waitChildren();
    if (tuples == null) {
      tuples = JdbcAccessMethod.tupleBatchIteratorFromQuery(jdbcInfo, baseSQL);
    }
    if (tuples.hasNext()) {
      return tuples.next();
    } else {
      return null;
    }
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
  }

  @Override
  public void setChildren(final Operator[] children) {
    this.children = children;
  }

  @Override
  public void cleanup() {
    tuples = null;
  }
}
