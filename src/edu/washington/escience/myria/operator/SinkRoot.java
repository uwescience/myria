package edu.washington.escience.myria.operator;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * A root operator that simply count the number of results and drop them.
 *
 * This RootOperator is a reasonable RootOperator for master plans which are not aiming at importing data into workers.
 * */
public class SinkRoot extends RootOperator {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  /**
   * Number of tuples.
   * */
  private long cnt;

  /**
   * The maximum number of tuples to read. If limit <= 0, all tuples will be read. Note that this is only accurate to
   * the nearest TupleBatch over limit.
   */
  private long limit;

  /**
   * @return count.
   * */
  public final long getCount() {
    return cnt;
  }

  /**
   * @param child the child.
   * */
  public SinkRoot(final Operator child) {
    super(child);
    setLimit(0);
  }

  /**
   * @param child the child.
   * @param limit the limit of the number of tuples this operator will absorb. If limit <= 0, all tuples will be read.
   *          Note that this is only accurate to the nearest TupleBatch over limit.
   * */
  public SinkRoot(final Operator child, final long limit) {
    super(child);
    setLimit(limit);
  }

  @Override
  protected final void consumeTuples(final TupleBatch tuples) throws DbException {
    cnt += tuples.numTuples();
    if (limit > 0 && cnt >= limit) {
      setEOS();
    }
  }

  @Override
  protected void childEOS() throws DbException {}

  @Override
  protected void childEOI() throws DbException {}

  @Override
  protected final void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    cnt = 0;
  }

  @Override
  protected void cleanup() throws DbException {}

  /**
   * Set the limit of the number of tuples this operator will absorb. If limit <= 0, all tuples will be read. Note that
   * this is only accurate to the nearest TupleBatch over limit.
   *
   * @param limit the number of tuples this operator will absorb. If limit <= 0, all tuples will be read. Note that this
   *          is only accurate to the nearest TupleBatch over limit.
   */
  public final void setLimit(final long limit) {
    this.limit = limit;
  }
}
