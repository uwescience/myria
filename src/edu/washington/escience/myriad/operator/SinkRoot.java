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
   * get count.
   * */
  public long getCount() {
    return cnt;
  }

  public SinkRoot(final Operator child) {
    super(child);
  }

  @Override
  protected void consumeTuples(final TupleBatch tuples) throws DbException {
    cnt += tuples.numTuples();
  }

  @Override
  protected void childEOS() throws DbException {
  }

  @Override
  protected void childEOI() throws DbException {
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    cnt = 0;
  }

  @Override
  protected void cleanup() throws DbException {
  }

}
