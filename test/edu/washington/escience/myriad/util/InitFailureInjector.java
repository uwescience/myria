package edu.washington.escience.myriad.util;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.operator.Operator;

/**
 * Inject a random failure. The injection is conducted as the following:<br/>
 * <ul>
 * <li>At the time the operator is opened, the delay starts to count.</li>
 * <li>Do nothing before delay expires.</li>
 * <li>After delay expires, for each second, randomly decide if a failure should be injected, i.e. throw an
 * {@link InjectedFailureException}, according to the failure probability.</li>
 * </ul>
 * */
public class InitFailureInjector extends Operator {

  /**
   * Logger.
   * */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(InitFailureInjector.class.getName());

  /**
   * Child.
   * */
  private Operator child;

  /**
   * @param delay the delay
   * @param delayUnit the timeunit of the delay
   * @param failureProbabilityPerSecond per second failure probability.
   * @param child the child operator.
   * */
  public InitFailureInjector(final Operator child) {
    this.child = child;
  }

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  @Override
  public final Operator[] getChildren() {
    return new Operator[] { child };
  }

  @Override
  protected final void init(final ImmutableMap<String, Object> initProperties) throws DbException {
    throw new InjectedFailureException("Failure in init.");
  }

  @Override
  protected final void cleanup() throws DbException {
  }

  @Override
  protected final TupleBatch fetchNextReady() throws DbException {
    return child.nextReady();
  }

  @Override
  public final Schema getSchema() {
    return child.getSchema();
  }

  @Override
  public final void setChildren(final Operator[] children) {
    child = children[0];
  }

}
