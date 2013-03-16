package edu.washington.escience.myriad.faulttolerance;

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

  private Operator child;
  private final long delayInMS;

  public DelayInjector(long delay, TimeUnit unit, Operator child) {
    this.child = child;
    delayInMS = unit.toMillis(delay);
  }

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  @Override
  protected TupleBatch fetchNext() throws DbException, InterruptedException {
    TupleBatch tb = child.next();
    Thread.sleep(delayInMS);
    return tb;
  }

  @Override
  public Operator[] getChildren() {
    return new Operator[] { child };
  }

  @Override
  protected void init(final ImmutableMap<String, Object> initProperties) throws DbException {
  }

  @Override
  protected void cleanup() throws DbException {
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    TupleBatch tb = child.nextReady();
    try {
      Thread.sleep(delayInMS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return tb;
  }

  @Override
  public Schema getSchema() {
    return child.getSchema();
  }

  @Override
  public void setChildren(Operator[] children) {
    child = children[0];
  }

}
