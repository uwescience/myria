package edu.washington.escience.myria.operator;

import java.util.Iterator;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.accessmethod.SQLiteAccessMethod;
import edu.washington.escience.myria.accessmethod.SQLiteInfo;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Wait the children to finish and retrieve data from SQLite.
 * */
public class SQLiteSQLProcessor extends Operator {

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
   * The output schema.
   * */
  private final Schema schema;
  /**
   * SQL template.
   * */
  private final String baseSQL;
  /**
   * The SQLite DB filepath.
   * */
  private SQLiteInfo sqliteInfo;

  /**
   * Construct a new SQLiteQueryScan object.
   * 
   * @param baseSQL the selection query.
   * @param schema the Schema of the returned tuples.
   * @param children to wait.
   */
  public SQLiteSQLProcessor(final String baseSQL, final Schema schema, final Operator[] children) {
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
   * @throws DbException if any operator processing error occurs
   * */
  private void waitChildrenReady() throws DbException {
    for (final Operator child : children) {
      while (!child.eos() && (child.nextReady() != null)) {
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
  public final void cleanup() {
    tuples = null;
  }

  /**
   * @return get data from SQLite.
   * @throws DbException error.
   * */
  protected final TupleBatch fetchNext() throws DbException {
    if (tuples == null) {
      tuples = SQLiteAccessMethod.tupleBatchIteratorFromQuery(sqliteInfo, baseSQL, schema);
    }
    if (tuples.hasNext()) {
      return tuples.next();
    } else {
      return null;
    }
  }

  @Override
  protected final TupleBatch fetchNextReady() throws DbException {
    if (allChildrenEOS) {
      return fetchNext();
    } else {
      waitChildrenReady();
      if (allChildrenEOS) {
        return fetchNext();
      }
    }
    return null;
  }

  @Override
  public final Schema generateSchema() {
    return schema;
  }

  @Override
  public final void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    sqliteInfo = (SQLiteInfo) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_DATABASE_CONN_INFO);
    if (sqliteInfo == null) {
      throw new DbException("Unable to instantiate SQLiteQueryScan on non-sqlite worker");
    }
    tuples = null;
  }

  @Override
  public final void setChildren(final Operator[] children) {
    this.children = children;
  }

}
