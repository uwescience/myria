package edu.washington.escience.myria.operator.failures;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.UnaryOperator;
import edu.washington.escience.myria.storage.TupleBatch;

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
  protected final void init(final ImmutableMap<String, Object> initProperties) throws DbException {}

  @Override
  protected final void cleanup() throws DbException {
    throw new InjectedFailureException("Failure in cleanup.");
  }

  @Override
  protected final TupleBatch fetchNextReady() throws DbException {
    return getChild().nextReady();
  }

  @Override
  public final Schema generateSchema() {
    Operator child = getChild();
    if (child == null) {
      return null;
    }
    return child.getSchema();
  }
}
