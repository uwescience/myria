package edu.washington.escience.myriad.operator;

import java.util.Iterator;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.MyriaConstants;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.accessmethod.JdbcAccessMethod;
import edu.washington.escience.myriad.accessmethod.JdbcInfo;

/**
 * Wait the children to finish and retrieve data from JDBC.
 * */
public class JdbcSQLProcessor extends Operator {

  /**
   * the children.
   * */
  private Operator[] children;
  /**
   * If all children have meet EOS.
   * */
  private boolean allChildrenEOS = false;

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * Iterate over data from the JDBC database.
   * */
  private Iterator<TupleBatch> tuples;
  /**
   * The result schema.
   * */
  private final Schema schema;
  /** The information for the JDBC connection. */
  private final JdbcInfo jdbcInfo;
  /**
   * the SQL template.
   * */
  private final String baseSQL;

  /**
   * 
   * @param jdbcInfo information for creating JDBC data connection.
   * @param baseSQL the query SQL template.
   * @param schema the Schema of the returned tuples.
   * @param children to wait.
   */
  public JdbcSQLProcessor(final JdbcInfo jdbcInfo, final String baseSQL, final Schema schema, final Operator[] children) {
    this.jdbcInfo = jdbcInfo;
    this.baseSQL = baseSQL;
    this.schema = schema;
    this.children = children;
  }

  @Override
  public final Operator[] getChildren() {
    return children;
  }

  /**
   * Wait children to finish.
   * 
   * @throws InterruptedException if interrupted.
   * @throws DbException if any operator processing error occurs
   * */
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

  /**
   * Wait children to finish.
   * 
   * @throws DbException if any operator processing error occurs
   * */
  private void waitChildrenReady() throws DbException {
    for (final Operator child : children) {
      while (!child.eos() && child.nextReady() != null) {
        try {
          Thread.sleep(MyriaConstants.SHORT_WAITING_INTERVAL_MS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
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
  protected final TupleBatch fetchNextReady() throws DbException {
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
  protected final TupleBatch fetchNext() throws DbException, InterruptedException {
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
  public final Schema getSchema() {
    return schema;
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
  }

  @Override
  public final void setChildren(final Operator[] children) {
    this.children = children;
  }

  @Override
  public final void cleanup() {
    tuples = null;
  }
}
