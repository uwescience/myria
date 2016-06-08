package edu.washington.escience.myria.operator.failures;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.UnaryOperator;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Injects an {@link InjectedFailureException} during initialization.
 */
public class InitFailureInjector extends UnaryOperator {

  /**
   * @param child the child operator.
   * */
  public InitFailureInjector(final Operator child) {
    super(child);
  }

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  @Override
  protected final void init(final ImmutableMap<String, Object> initProperties) throws DbException {
    throw new InjectedFailureException("Failure in init.");
  }

  @Override
  protected final void cleanup() throws DbException {}

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
