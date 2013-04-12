package edu.washington.escience.myriad.operator;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;

/**
 * Emit an EOS directly and do nothing else.
 * */
public class EOSSource extends LeafOperator {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  @Override
  protected final TupleBatch fetchNext() throws DbException, InterruptedException {
    return null;
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {

  }

  @Override
  protected void cleanup() throws DbException {

  }

  @Override
  protected final TupleBatch fetchNextReady() throws DbException {
    setEOS();
    return null;
  }

  @Override
  public final Schema getSchema() {
    return Schema.EMPTY_SCHEMA;
  }

}
