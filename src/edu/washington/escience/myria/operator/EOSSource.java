package edu.washington.escience.myria.operator;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Emit an EOS directly and do nothing else.
 * */
public class EOSSource extends LeafOperator {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {}

  @Override
  protected void cleanup() throws DbException {}

  @Override
  protected final TupleBatch fetchNextReady() throws DbException {
    setEOS();
    return null;
  }

  @Override
  public final Schema generateSchema() {
    return Schema.EMPTY_SCHEMA;
  }
}
