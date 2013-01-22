package edu.washington.escience.myriad.parallel;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

/**
 * A wrapper close future. <br>
 * It works as the following: <br>
 * 1. wait until the condition is set<br>
 * 2. if condition is true, pass all the operations directly to the wrapped close future.<br>
 * 3. else set the wrapper future as done and succeed.
 * */
public class ConditionCloseFuture implements ChannelFuture {

  private final Channel channel;
  private volatile boolean conditionSetFlag = false;
  private volatile boolean condition = false;
  private final Object conditionSetLock = new Object();

  private final ConcurrentHashMap<ChannelFutureListener, ChannelFutureListener> listeners =
      new ConcurrentHashMap<ChannelFutureListener, ChannelFutureListener>();

  public ConditionCloseFuture(final Channel channel) {
    this.channel = channel;
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
          t.printStackTrace();
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
      } else {
        return null;
      }
    }
  }

  @Override
  public Channel getChannel() {
    return channel;
  }

  @Override
  public boolean isCancelled() {
    if (!conditionSetFlag) {
      return false;
    } else {
      if (condition) {
        return channel.getCloseFuture().isCancelled();
      } else {
        return false;
      }
    }
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
        return true;
      }
    }
  }

  @Override
  public void removeListener(final ChannelFutureListener listener) {
    synchronized (conditionSetLock) {
      if (!conditionSetFlag) {
        listeners.remove(listener);
      }
    }
    if (conditionSetFlag) {
      if (condition) {
        channel.getCloseFuture().removeListener(listener);
      }
    }
  }

  @Override
  public ChannelFuture rethrowIfFailed() throws Exception {
    throw new UnsupportedOperationException();
  }

  public void setCondition(final boolean conditionSatisfied) {
    synchronized (conditionSetLock) {
      condition = conditionSatisfied;
      conditionSetFlag = true;
      if (condition) {
        for (final ChannelFutureListener l : listeners.keySet()) {
          channel.getCloseFuture().addListener(l);
        }
      } else {
        listeners.clear();
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
    throw new UnsupportedOperationException();
  }

  @Override
  public ChannelFuture syncUninterruptibly() {
    throw new UnsupportedOperationException();
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
