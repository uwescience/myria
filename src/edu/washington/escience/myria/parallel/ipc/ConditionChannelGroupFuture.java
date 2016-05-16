package edu.washington.escience.myria.parallel.ipc;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.ChannelGroupFutureListener;
import org.jboss.netty.channel.group.DefaultChannelGroup;

/**
 * A {@link ChannelGroupFuture} that waits for a condition to be satisfied before actually do the channel group event
 * checking.
 * */
public class ConditionChannelGroupFuture implements ChannelGroupFuture {
  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(ConditionChannelGroupFuture.class);

  /**
   * the actual channel group future that monitors the channel group event.
   * */
  private volatile ChannelGroupFuture backedChannelGroupFuture;
  /**
   * if the condition is set.
   * */
  private volatile boolean conditionSetFlag = false;
  /**
   * if the condition is satisfied.
   * */
  private volatile boolean condition = false;
  /**
   * protect the condition .
   * */
  private final Object conditionSetLock = new Object();

  /**
   * empty channel group.
   * */
  public static final ChannelGroup EMPTY_GROUP = new DefaultChannelGroup();

  /**
   * listeners which will be called back if the action is done.
   * */
  private final ConcurrentHashMap<ChannelGroupFutureListener, ChannelGroupFutureListener>
      listeners = new ConcurrentHashMap<ChannelGroupFutureListener, ChannelGroupFutureListener>();

  /**
   * @param wrappedCGF the wrapped ChannelGroupFuture
   * */
  public final void setBackedChannelGroupFuture(final ChannelGroupFuture wrappedCGF) {
    backedChannelGroupFuture = wrappedCGF;
  }

  @Override
  public final void addListener(final ChannelGroupFutureListener listener) {
    synchronized (conditionSetLock) {
      if (!conditionSetFlag) {
        listeners.put(listener, listener);
        return;
      }
    }
    if (condition) {
      backedChannelGroupFuture.addListener(listener);
    } else {
      try {
        listener.operationComplete(this);
      } catch (final Throwable t) {
        LOGGER.warn("Exception occured when executing ChannelGroupFutureListener", t);
      }
    }
  }

  /**
   * Make sure that the condition variable is set before trying to use its value.
   * */
  private void ensureConditionSet() {
    if (!conditionSetFlag) {
      throw new IllegalStateException("Condition not set yet");
    }
  }

  @Override
  public final ChannelGroup getGroup() {
    ensureConditionSet();
    if (condition) {
      return backedChannelGroupFuture.getGroup();
    } else {
      return EMPTY_GROUP;
    }
  }

  @Override
  public final ChannelFuture find(final Integer channelId) {
    ensureConditionSet();
    if (condition) {
      return backedChannelGroupFuture.find(channelId);
    } else {
      return null;
    }
  }

  @Override
  public final ChannelFuture find(final Channel channel) {
    if (channel == null) {
      return null;
    }
    return this.find(channel.getId());
  }

  @Override
  public final boolean isCompleteSuccess() {
    ensureConditionSet();
    if (condition) {
      return backedChannelGroupFuture.isCompleteSuccess();
    }
    return false;
  }

  @Override
  public final boolean isPartialSuccess() {
    ensureConditionSet();
    if (condition) {
      return backedChannelGroupFuture.isPartialSuccess();
    }
    return false;
  }

  @Override
  public final boolean isCompleteFailure() {
    ensureConditionSet();
    if (condition) {
      return backedChannelGroupFuture.isCompleteFailure();
    }
    return true;
  }

  @Override
  public final boolean isPartialFailure() {
    ensureConditionSet();
    if (condition) {
      return backedChannelGroupFuture.isPartialFailure();
    }
    return false;
  }

  @Override
  public final Iterator<ChannelFuture> iterator() {
    ensureConditionSet();
    if (condition) {
      return backedChannelGroupFuture.iterator();
    } else {
      return null;
    }
  }

  @Override
  public final ChannelGroupFuture await() throws InterruptedException {
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
    waitForConditionSet(-1, -1);
    if (condition) {
      backedChannelGroupFuture.await();
    }
    return this;
  }

  @Override
  public final boolean await(final long timeoutMillis) throws InterruptedException {
    return this.await(timeoutMillis, MILLISECONDS);
  }

  @Override
  public final boolean await(final long timeout, final TimeUnit unit) throws InterruptedException {

    final long nano = unit.toNanos(timeout);
    final long milli = NANOSECONDS.toMillis(nano);
    final int nanoRemain = (int) (nano - MILLISECONDS.toNanos(milli));
    final long remain = nano - waitForConditionSet(milli, nanoRemain);

    if (conditionSetFlag) {
      if (condition) {
        return backedChannelGroupFuture.await(remain, NANOSECONDS);
      } else {
        return true;
      }
    } else {
      return false;
    }
  }

  @Override
  public final ChannelGroupFuture awaitUninterruptibly() {
    waitForConditionSetUninterruptibly(-1, -1);
    if (condition) {
      backedChannelGroupFuture.awaitUninterruptibly();
    }
    return this;
  }

  @Override
  public final boolean awaitUninterruptibly(final long timeoutMillis) {
    return this.awaitUninterruptibly(timeoutMillis, MILLISECONDS);
  }

  @Override
  public final boolean awaitUninterruptibly(final long timeout, final TimeUnit unit) {
    final long nano = unit.toNanos(timeout);
    final long milli = NANOSECONDS.toMillis(nano);
    final int nanoRemain = (int) (nano - MILLISECONDS.toNanos(milli));
    final long remain = nano - waitForConditionSetUninterruptibly(milli, nanoRemain);

    if (conditionSetFlag) {
      if (condition) {
        return backedChannelGroupFuture.awaitUninterruptibly(remain, NANOSECONDS);
      } else {
        return true;
      }
    } else {
      return false;
    }
  }

  @Override
  public final boolean isDone() {
    if (!conditionSetFlag) {
      return false;
    } else {
      if (condition) {
        return backedChannelGroupFuture.isDone();
      } else {
        return true;
      }
    }
  }

  @Override
  public final void removeListener(final ChannelGroupFutureListener listener) {
    synchronized (conditionSetLock) {
      if (!conditionSetFlag) {
        listeners.remove(listener);
      }
    }
    if (conditionSetFlag) {
      if (condition) {
        backedChannelGroupFuture.removeListener(listener);
      }
    }
  }

  /**
   * Set the condition value.
   *
   * @param conditionSatisfied if the condition is satisfied.
   * */
  public final void setCondition(final boolean conditionSatisfied) {
    synchronized (conditionSetLock) {
      condition = conditionSatisfied;
      conditionSetFlag = true;
      if (condition) {
        for (final ChannelGroupFutureListener l : listeners.keySet()) {
          backedChannelGroupFuture.addListener(l);
        }
        listeners.clear();
      } else {
        for (final ChannelGroupFutureListener l : listeners.keySet()) {
          try {
            l.operationComplete(this);
          } catch (final Throwable t) {
            LOGGER.warn("Exception occured when executing ChannelGroupFutureListener", t);
          }
        }
      }
      conditionSetLock.notifyAll();
    }
  }

  /**
   * @return the time elapse in nano seconds in this method.
   * @param timeoutMS timeout in milliseconds
   * @param nanos remaining nanos.
   * @throws InterruptedException if interrupted.
   * */
  private long waitForConditionSet(final long timeoutMS, final int nanos)
      throws InterruptedException {
    final long start = System.nanoTime();
    synchronized (conditionSetLock) {
      if (!conditionSetFlag) {
        if (timeoutMS >= 0) {
          conditionSetLock.wait(timeoutMS, nanos);
        } else {
          conditionSetLock.wait();
        }
      }
    }
    return System.nanoTime() - start;
  }

  /**
   * @return the time elapse in nano seconds in this method.
   * @param timeoutMS timeout in milliseconds
   * @param nanos remaining nanos.
   * */
  private long waitForConditionSetUninterruptibly(final long timeoutMS, final int nanos) {
    boolean interrupted = false;
    final long start = System.nanoTime();
    synchronized (conditionSetLock) {
      if (!conditionSetFlag) {
        try {
          if (timeoutMS >= 0) {
            conditionSetLock.wait(timeoutMS, nanos);
          } else {
            conditionSetLock.wait();
          }
        } catch (final InterruptedException e) {
          interrupted = true;
        }
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    return System.nanoTime() - start;
  }
}
