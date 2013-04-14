package edu.washington.escience.myriad.operator;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.TupleBatch;

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
  }

  @Override
  protected final void consumeTuples(final TupleBatch tuples) throws DbException {
    cnt += tuples.numTuples();
  }

  @Override
  protected void childEOS() throws DbException {
  }

  @Override
  protected void childEOI() throws DbException {
  }

  @Override
  protected final void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    cnt = 0;
  }

  @Override
  protected void cleanup() throws DbException {
  }

}
