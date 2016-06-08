package edu.washington.escience.myria.operator.failures;

import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.UnaryOperator;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Inject delay in processing each TupleBatch.
 * */
public class DelayInjector extends UnaryOperator {

  /**
   * Delay in milliseconds.
   * */
  private final long delayInMS;

  /** if delay in fetchNextReady() for only one time. */
  private final boolean onetime;

  /** if it's the first time to call fetchNextReady(). */
  private boolean firsttime = true;

  /**
   * @param delay the delay
   * @param unit time unit of the delay
   * @param child the child.
   * */
  public DelayInjector(final long delay, final TimeUnit unit, final Operator child) {
    this(delay, unit, child, false);
  }

  /**
   * @param delay the delay
   * @param unit time unit of the delay
   * @param child the child.
   * @param onetime if delay only at the first time of fetchNextReady.
   * */
  public DelayInjector(
      final long delay, final TimeUnit unit, final Operator child, final boolean onetime) {
    super(child);
    this.onetime = onetime;
    delayInMS = unit.toMillis(delay);
  }

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  @Override
  protected final void init(final ImmutableMap<String, Object> initProperties) throws DbException {}

  @Override
  protected final void cleanup() throws DbException {}

  @Override
  protected final TupleBatch fetchNextReady() throws DbException {
    TupleBatch tb = getChild().nextReady();
    try {
      if (onetime && firsttime || !onetime) {
        Thread.sleep(delayInMS);
        firsttime = false;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return tb;
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
