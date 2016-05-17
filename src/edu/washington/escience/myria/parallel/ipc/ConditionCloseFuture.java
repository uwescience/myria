package edu.washington.escience.myria.parallel.ipc;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

/**
 * A wrapper close future. <br>
 * It works as the following: <br>
 * 1. wait until the condition is set<br>
 * 2. if condition is true, pass all the operations directly to the wrapped close future.<br>
 * 3. else set the wrapper future as done and treat it as fail by default, or succeed if specified in constructor.
 * */
public class ConditionCloseFuture implements ChannelFuture {

  /**
   * The owner channel.
   * */
  private final Channel channel;
  /**
   * If the condition is set or not.
   * */
  private volatile boolean conditionSetFlag = false;
  /**
   * the condition is satisfied or not.
   * */
  private volatile boolean condition = false;
  /**
   * condition set lock.
   * */
  private final Object conditionSetLock = new Object();
  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(ConditionCloseFuture.class);
  /**
   * If the condition is unsatisfied, treat the future as a failure.
   * */
  private final boolean asFail;

  /**
   * listeners to notify.
   * */
  private final ConcurrentHashMap<ChannelFutureListener, ChannelFutureListener> listeners =
      new ConcurrentHashMap<ChannelFutureListener, ChannelFutureListener>();

  /**
   * @param channel the owner channel.
   * */
  public ConditionCloseFuture(final Channel channel) {
    this.channel = channel;
    asFail = true;
  }

  /**
   * @param channel the owner channel.
   * @param conditionUnSatisfiedAsSucceed if the condition is unsatisfied the future should be treated as a succeed or
   *          not
   * */
  public ConditionCloseFuture(final Channel channel, final boolean conditionUnSatisfiedAsSucceed) {
    this.channel = channel;
    asFail = !conditionUnSatisfiedAsSucceed;
  }

  @Override
  public final void addListener(final ChannelFutureListener listener) {
    synchronized (conditionSetLock) {
      if (!conditionSetFlag) {
        listeners.put(listener, listener);
        return;
      }
    }
    if (conditionSetFlag) {
      if (condition) {
        channel.getCloseFuture().addListener(listener);
      } else {
        try {
          listener.operationComplete(this);
        } catch (final Throwable t) {
          LOGGER.warn("Exception occured when executing ChannelGroupFutureListener", t);
        }
      }
    }
  }

  @Override
  public final ChannelFuture await() throws InterruptedException {
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
    waitForConditionSet(-1, -1);
    if (condition) {
      channel.getCloseFuture().await();
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
    final long milli = TimeUnit.NANOSECONDS.toMillis(nano);
    final int nanoRemain = (int) (nano - TimeUnit.MILLISECONDS.toNanos(milli));
    final long remain = nano - waitForConditionSet(milli, nanoRemain);

    if (conditionSetFlag) {
      if (condition) {
        return channel.getCloseFuture().await(remain, TimeUnit.NANOSECONDS);
      } else {
        return true;
      }
    } else {
      return false;
    }
  }

  @Override
  public final ChannelFuture awaitUninterruptibly() {
    waitForConditionSetUninterruptibly(-1, -1);
    if (condition) {
      channel.getCloseFuture().awaitUninterruptibly();
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
    final long milli = TimeUnit.NANOSECONDS.toMillis(nano);
    final int nanoRemain = (int) (nano - TimeUnit.MILLISECONDS.toNanos(milli));
    final long remain = nano - waitForConditionSetUninterruptibly(milli, nanoRemain);

    if (conditionSetFlag) {
      if (condition) {
        return channel.getCloseFuture().awaitUninterruptibly(remain, TimeUnit.NANOSECONDS);
      } else {
        return true;
      }
    } else {
      return false;
    }
  }

  @Override
  public final boolean cancel() {
    return channel.getCloseFuture().cancel();
  }

  @Override
  public final Throwable getCause() {
    if (!conditionSetFlag) {
      return null;
    } else {
      if (condition) {
        return channel.getCloseFuture().getCause();
      } else if (!asFail) {
        return null;
      } else {
        return new ConditionUnsatisfiedException();
      }
    }
  }

  @Override
  public final Channel getChannel() {
    return channel;
  }

  @Override
  public final boolean isCancelled() {
    return channel.getCloseFuture().isCancelled();
  }

  @Override
  public final boolean isDone() {
    if (!conditionSetFlag) {
      return false;
    } else {
      if (condition) {
        return channel.getCloseFuture().isDone();
      } else {
        return true;
      }
    }
  }

  @Override
  public final boolean isSuccess() {
    if (!conditionSetFlag) {
      return false;
    } else {
      if (condition) {
        return channel.getCloseFuture().isSuccess();
      } else {
        return !asFail;
      }
    }
  }

  @Override
  public final void removeListener(final ChannelFutureListener listener) {
    synchronized (conditionSetLock) {
      if (!conditionSetFlag) {
        listeners.remove(listener);
        return;
      }
    }
    if (condition) {
      channel.getCloseFuture().removeListener(listener);
    }
  }

  @Override
  @Deprecated
  public final ChannelFuture rethrowIfFailed() throws Exception {
    if (!conditionSetFlag) {
      return this;
    }
    if (condition) {
      channel.getCloseFuture().rethrowIfFailed();
    } else if (asFail) {
      throw new ConditionUnsatisfiedException();
    }
    return this;
  }

  /**
   * set the condition.
   *
   * @param conditionSatisfied if the condition is true
   * */
  public final void setCondition(final boolean conditionSatisfied) {
    synchronized (conditionSetLock) {
      condition = conditionSatisfied;
      conditionSetFlag = true;
      if (condition) {
        for (final ChannelFutureListener l : listeners.keySet()) {
          channel.getCloseFuture().addListener(l);
        }
        listeners.clear();
      } else {
        for (final ChannelFutureListener l : listeners.keySet()) {
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

  @Override
  public final boolean setFailure(final Throwable cause) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final boolean setProgress(final long amount, final long current, final long total) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final boolean setSuccess() {
    throw new UnsupportedOperationException();
  }

  @Override
  public final ChannelFuture sync() throws InterruptedException {
    await();
    try {
      rethrowIfFailed();
    } catch (Exception e) {
      throw new ChannelException(e);
    }
    return this;
  }

  @Override
  public final ChannelFuture syncUninterruptibly() {
    this.awaitUninterruptibly();
    try {
      rethrowIfFailed();
    } catch (Exception e) {
      throw new ChannelException(e);
    }
    return this;
  }

  /**
   * Wait the condition to be set for at most timeoutMS milliseconds and nanos nanoseconds.
   *
   * @param timeoutMS milliseconds
   * @param nanos nanoseconds.
   * @throws InterruptedException if interrupted.
   * @return the amount of waiting time remain in nanoseconds.
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
   * Wait the condition to be set for at most timeoutMS milliseconds and nanos nanoseconds without interruption.
   *
   * @param timeoutMS milliseconds
   * @param nanos nanoseconds.
   * @return the amount of waiting time remain in nanoseconds.
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
