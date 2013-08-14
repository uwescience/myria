package edu.washington.escience.myriad.util;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.UnaryOperator;

/**
 * Injects an {@link InjectedFailureException} during cleanup.
 */
public class CleanupFailureInjector extends UnaryOperator {

  /**
   * @param child the child operator.
   */
  public CleanupFailureInjector(final Operator child) {
    super(child);
  }

  /**
   * Required for Java serialization.
   */
  private static final long serialVersionUID = 1L;

  @Override
  protected final void init(final ImmutableMap<String, Object> initProperties) throws DbException {
  }

  @Override
  protected final void cleanup() throws DbException {
    throw new InjectedFailureException("Failure in cleanup.");
  }

  @Override
  protected final TupleBatch fetchNextReady() throws DbException {
    return getChild().nextReady();
  }

  @Override
  public final Schema getSchema() {
    return getChild().getSchema();
  }
}
