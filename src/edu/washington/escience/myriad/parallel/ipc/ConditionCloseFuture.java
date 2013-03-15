package edu.washington.escience.myriad.parallel.ipc;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

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

  private final Channel channel;
  private volatile boolean conditionSetFlag = false;
  private volatile boolean condition = false;
  private final Object conditionSetLock = new Object();
  private static final Logger logger = Logger.getLogger(ConditionCloseFuture.class.getName());
  private final boolean asFail;

  private final ConcurrentHashMap<ChannelFutureListener, ChannelFutureListener> listeners =
      new ConcurrentHashMap<ChannelFutureListener, ChannelFutureListener>();

  public ConditionCloseFuture(final Channel channel) {
    this.channel = channel;
    asFail = true;
  }

  public ConditionCloseFuture(final Channel channel, final boolean conditionUnSatisfiedAsSucceed) {
    this.channel = channel;
    asFail = !conditionUnSatisfiedAsSucceed;
  }

  @Override
  public void addListener(final ChannelFutureListener listener) {
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
          logger.log(Level.WARNING, "Exception occured when executing ChannelGroupFutureListener", t);
        }
      }
    }
  }

  @Override
  public ChannelFuture await() throws InterruptedException {
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
  public boolean await(final long timeoutMillis) throws InterruptedException {
    return this.await(timeoutMillis, MILLISECONDS);
  }

  @Override
  public boolean await(final long timeout, final TimeUnit unit) throws InterruptedException {

    final long nano = unit.toNanos(timeout);
    final long milli = nano / 1000000;
    final int nanoRemain = (int) (nano - milli * 1000000);
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
  public ChannelFuture awaitUninterruptibly() {
    waitForConditionSetUninterruptibly(-1, -1);
    if (condition) {
      channel.getCloseFuture().awaitUninterruptibly();
    }
    return this;
  }

  @Override
  public boolean awaitUninterruptibly(final long timeoutMillis) {
    return this.awaitUninterruptibly(timeoutMillis, MILLISECONDS);
  }

  @Override
  public boolean awaitUninterruptibly(final long timeout, final TimeUnit unit) {
    final long nano = unit.toNanos(timeout);
    final long milli = nano / 1000000;
    final int nanoRemain = (int) (nano - milli * 1000000);
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
  public boolean cancel() {
    return channel.getCloseFuture().cancel();
  }

  @Override
  public Throwable getCause() {
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
  public Channel getChannel() {
    return channel;
  }

  @Override
  public boolean isCancelled() {
    return channel.getCloseFuture().isCancelled();
  }

  @Override
  public boolean isDone() {
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
  public boolean isSuccess() {
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
  public void removeListener(final ChannelFutureListener listener) {
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

  public void setCondition(final boolean conditionSatisfied) {
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
            logger.log(Level.WARNING, "Exception occured when executing ChannelGroupFutureListener", t);
          }
        }
      }
      conditionSetLock.notifyAll();
    }
  }

  @Override
  public boolean setFailure(final Throwable cause) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean setProgress(final long amount, final long current, final long total) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean setSuccess() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ChannelFuture sync() throws InterruptedException {
    await();
    try {
      rethrowIfFailed();
    } catch (Exception e) {
      throw new ChannelException(e);
    }
    return this;
  }

  @Override
  public ChannelFuture syncUninterruptibly() {
    this.awaitUninterruptibly();
    try {
      rethrowIfFailed();
    } catch (Exception e) {
      throw new ChannelException(e);
    }
    return this;
  }

  private long waitForConditionSet(final long timeoutMS, final int nanos) throws InterruptedException {
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
