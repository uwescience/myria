package edu.washington.escience.myria.util;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.UnaryOperator;

/**
 * Injects an {@link InjectedFailureException} during initialization. </ul>
 */
public class InitFailureInjector extends UnaryOperator {

  /**
   * @param delay the delay
   * @param delayUnit the timeunit of the delay
   * @param failureProbabilityPerSecond per second failure probability.
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
  protected final void cleanup() throws DbException {
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
