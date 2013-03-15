package edu.washington.escience.myriad.operator;

// import edu.washington.escience.Schema;
import java.util.Iterator;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.accessmethod.JdbcAccessMethod;

public class JdbcSQLProcessor extends Operator {

  private Operator[] children;
  private boolean allChildrenEOS = false;

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  private Iterator<TupleBatch> tuples;
  private final Schema schema;
  private final String driverClass;
  private final String connectionString;
  private final String baseSQL;
  private final String username;
  private final String password;

  public JdbcSQLProcessor(final String driverClass, final String connectionString, final String baseSQL,
      final Schema schema, final Operator[] children, final String username, final String password) {
    this.driverClass = driverClass;
    this.connectionString = connectionString;
    this.baseSQL = baseSQL;
    this.username = username;
    this.password = password;
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
      tuples = JdbcAccessMethod.tupleBatchIteratorFromQuery(driverClass, connectionString, baseSQL, username, password);
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
