package edu.washington.escience.myriad.util;

import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.operator.Operator;

/**
 * Inject delay in processing each TupleBatch.
 * */
public class DelayInjector extends Operator {

  /**
   * child.
   * */
  private Operator child;

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
    this.child = child;
    onetime = false;
    delayInMS = unit.toMillis(delay);
  }

  /**
   * @param delay the delay
   * @param unit time unit of the delay
   * @param child the child.
   * @param onetime if delay only at the first time of fetchNextReady.
   * */
  public DelayInjector(final long delay, final TimeUnit unit, final Operator child, final boolean onetime) {
    this.child = child;
    this.onetime = onetime;
    delayInMS = unit.toMillis(delay);
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
  }

  @Override
  protected final void cleanup() throws DbException {
  }

  @Override
  protected final TupleBatch fetchNextReady() throws DbException {
    TupleBatch tb = child.nextReady();
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
  public final Schema getSchema() {
    return child.getSchema();
  }

  @Override
  public final void setChildren(final Operator[] children) {
    child = children[0];
  }

}
