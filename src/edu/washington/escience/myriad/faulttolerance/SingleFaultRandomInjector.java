package edu.washington.escience.myriad.faulttolerance;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.operator.Operator;

/**
 * Inject delay in processing each TupleBatch.
 * */
public class SingleFaultRandomInjector extends Operator {

  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(SingleFaultRandomInjector.class
      .getName());

  private Operator child;
  private final double failureProbabilityPerSecond;
  private volatile boolean hasFailed;
  private volatile boolean toFail;
  private volatile Thread failureInjectThread;
  private final long delayInMS;

  public SingleFaultRandomInjector(final long delay, final TimeUnit delayUnit,
      final double failureProbabilityPerSecond, final Operator child) {
    this.child = child;
    this.failureProbabilityPerSecond = failureProbabilityPerSecond;
    hasFailed = false;
    toFail = false;
    delayInMS = delayUnit.toMillis(delay);
  }

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  @Override
  protected TupleBatch fetchNext() throws DbException, InterruptedException {
    if (toFail) {
      toFail = false;
      throw new InjectedFailureException("Failure injected by " + this);
    }
    return child.next();
  }

  @Override
  public Operator[] getChildren() {
    return new Operator[] { child };
  }

  @Override
  protected void init(ImmutableMap<String, Object> initProperties) throws DbException {
    toFail = false;
    if (!hasFailed) {
      failureInjectThread = new Thread() {
        @Override
        public void run() {
          Random r = new Random();
          try {
            Thread.sleep(delayInMS);
          } catch (InterruptedException e) {
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug(SingleFaultRandomInjector.this + " exit during delay.");
            }
            return;
          }
          while (true) {
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(SingleFaultRandomInjector.this + " exit during 1 second sleep.");
              }
              return;
            }
            if (r.nextDouble() < failureProbabilityPerSecond) {
              toFail = true;
              hasFailed = true;
              return;
            }
          }
        }
      };
      failureInjectThread.start();
    } else {
      failureInjectThread = null;
    }

  }

  @Override
  protected void cleanup() throws DbException {
    if (failureInjectThread != null) {
      failureInjectThread.interrupt();
      failureInjectThread = null;
    }
    toFail = false;
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    if (toFail) {
      toFail = false;
      throw new InjectedFailureException("Failure injected by " + this);
    }
    return child.nextReady();
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
