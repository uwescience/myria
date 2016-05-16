package edu.washington.escience.myria.operator.failures;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.UnaryOperator;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Inject a random failure. The injection is conducted as the following:<br/>
 * <ul>
 * <li>At the time the operator is opened, the delay starts to count.</li>
 * <li>Do nothing before delay expires.</li>
 * <li>After delay expires, for each second, randomly decide if a failure should be injected, i.e. throw an
 * {@link InjectedFailureException}, according to the failure probability.</li>
 * </ul>
 * */
public class SingleRandomFailureInjector extends UnaryOperator {

  /**
   * Logger.
   * */
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(SingleRandomFailureInjector.class.getName());

  /**
   * failure probability per second.
   * */
  private final double failureProbabilityPerSecond;

  /**
   * Record if a failure is already injected. The class injects only a single failure.
   * */
  private volatile boolean hasFailed;

  /**
   * Denoting if the next tuple retrieval event should trigger a failure.
   * */
  private volatile boolean toFail;

  /**
   * failure inject thread. This thread decides if a failure should be injected.
   * */
  private volatile Thread failureInjectThread;

  /**
   * delay in milliseconds.
   * */
  private final long delayInMS;

  /**
   * @param delay the delay
   * @param delayUnit the timeunit of the delay
   * @param failureProbabilityPerSecond per second failure probability.
   * @param child the child operator.
   * */
  public SingleRandomFailureInjector(
      final long delay,
      final TimeUnit delayUnit,
      final double failureProbabilityPerSecond,
      final Operator child) {
    super(child);
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
  protected final void init(final ImmutableMap<String, Object> initProperties) throws DbException {
    toFail = false;
    if (!hasFailed) {
      failureInjectThread =
          new Thread() {
            @Override
            public void run() {
              Random r = new Random();
              try {
                Thread.sleep(delayInMS);
              } catch (InterruptedException e) {
                if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug(SingleRandomFailureInjector.this + " exit during delay.");
                }
                return;
              }
              while (true) {
                if (r.nextDouble() < failureProbabilityPerSecond) {
                  toFail = true;
                  hasFailed = true;
                  return;
                }

                try {
                  Thread.sleep(TimeUnit.SECONDS.toMillis(1));
                } catch (InterruptedException e) {
                  if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(SingleRandomFailureInjector.this + " exit during 1 second sleep.");
                  }
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
  protected final void cleanup() throws DbException {
    if (failureInjectThread != null) {
      failureInjectThread.interrupt();
      failureInjectThread = null;
    }
    toFail = false;
  }

  @Override
  protected final TupleBatch fetchNextReady() throws DbException {
    if (toFail) {
      toFail = false;
      throw new InjectedFailureException("Failure injected by " + this);
    }
    return getChild().nextReady();
  }

  @Override
  protected final Schema generateSchema() {
    Operator child = getChild();
    if (child == null) {
      return null;
    }
    return child.getSchema();
  }
}
