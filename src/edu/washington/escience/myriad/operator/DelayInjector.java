package edu.washington.escience.myriad.operator;

import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;

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

  /**
   * @param delay the delay
   * @param unit time unit of the delay
   * @param child the child.
   * */
  public DelayInjector(final long delay, final TimeUnit unit, final Operator child) {
    this.child = child;
    delayInMS = unit.toMillis(delay);
  }

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  @Override
  protected final TupleBatch fetchNext() throws DbException, InterruptedException {
    TupleBatch tb = child.next();
    Thread.sleep(delayInMS);
    return tb;
  }

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
      Thread.sleep(delayInMS);
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
