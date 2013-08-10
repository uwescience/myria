/*
 * This file is a modification to the DefaultChannelFuture in Netty.
 * 
 * Copyright 2012 The Netty Project
 * 
 * The Netty Project licenses this file to you under the Apache License, version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the License at:
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package edu.washington.escience.myriad.parallel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.util.AttachmentableAdapter;

/**
 * The default {@link QueryFuture} implementation.
 */
public class DefaultQueryFuture extends AttachmentableAdapter implements QueryFuture {

  /**
   * logger.
   * */
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultQueryFuture.class);

  /**
   * The cancelled cause.
   * */
  private static final Throwable CANCELLED = new Throwable();

  /**
   * The owner query of this future, i.e. the future is for an operation on the query.
   * */
  private final QueryPartition query;

  /**
   * if the future is cancellable.
   * */
  private final boolean cancellable;

  /**
   * The first listener of all the list of listeners.
   * */
  private QueryFutureListener firstListener;
  /**
   * All other listeners.
   * */
  private List<QueryFutureListener> otherListeners;
  /**
   * Listeners for progress events.
   * */
  private List<QueryFutureProgressListener> progressListeners;
  /**
   * If the action is done.
   * */
  private boolean done;
  /**
   * If the action failes, what's the cause.
   * */
  private Throwable cause;

  /**
   * Thread safe. Guarded by this.
   * */
  private int waiters;

  /**
   * Creates a new instance.
   * 
   * @param query the {@link Query } associated with this future
   * @param cancellable {@code true} if and only if this future can be canceled
   */
  public DefaultQueryFuture(final QueryPartition query, final boolean cancellable) {
    this.query = query;
    this.cancellable = cancellable;
  }

  @Override
  public final QueryPartition getQuery() {
    return query;
  }

  @Override
  public final synchronized boolean isDone() {
    return done;
  }

  @Override
  public final synchronized boolean isSuccess() {
    return done && cause == null;
  }

  @Override
  public final synchronized Throwable getCause() {
    if (cause != CANCELLED) {
      return cause;
    } else {
      return null;
    }
  }

  @Override
  public final synchronized boolean isCancelled() {
    return cause == CANCELLED;
  }

  @Override
  public final QueryFuture addListener(final QueryFutureListener listener) {
    if (listener == null) {
      throw new NullPointerException("listener");
    }

    boolean notifyNow = false;
    synchronized (this) {
      if (done) {
        notifyNow = true;
      } else {
        if (firstListener == null) {
          firstListener = listener;
        } else {
          if (otherListeners == null) {
            otherListeners = new ArrayList<QueryFutureListener>(1);
          }
          otherListeners.add(listener);
        }

        if (listener instanceof QueryFutureProgressListener) {
          if (progressListeners == null) {
            progressListeners = new ArrayList<QueryFutureProgressListener>(1);
          }
          progressListeners.add((QueryFutureProgressListener) listener);
        }
      }
    }

    if (notifyNow) {
      notifyListener(listener);
    }
    return this;
  }

  @Override
  public final QueryFuture removeListener(final QueryFutureListener listener) {
    if (listener == null) {
      throw new NullPointerException("listener");
    }

    synchronized (this) {
      if (!done) {
        if (listener == firstListener) {
          if (otherListeners != null && !otherListeners.isEmpty()) {
            firstListener = otherListeners.remove(0);
          } else {
            firstListener = null;
          }
        } else if (otherListeners != null) {
          otherListeners.remove(listener);
        }

        if (listener instanceof QueryFutureProgressListener) {
          progressListeners.remove(listener);
        }
      }
    }
    return this;
  }

  @Override
  public final QueryFuture sync() throws InterruptedException, DbException {
    await();
    rethrowIfFailed0();
    return this;
  }

  @Override
  public final QueryFuture syncUninterruptibly() throws DbException {
    awaitUninterruptibly();
    rethrowIfFailed0();
    return this;
  }

  /**
   * .
   * 
   * @throws DbException any error will be wrapped into a DbException
   * */
  private void rethrowIfFailed0() throws DbException {
    Throwable causeLocal = getCause();
    if (causeLocal == null) {
      return;
    }

    if (causeLocal instanceof RuntimeException) {
      throw (RuntimeException) causeLocal;
    }

    if (causeLocal instanceof Error) {
      throw (Error) causeLocal;
    }

    throw new DbException(causeLocal);
  }

  @Override
  public final QueryFuture await() throws InterruptedException {
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }

    synchronized (this) {
      while (!done) {
        waiters++;
        try {
          wait();
        } finally {
          waiters--;
        }
      }
    }
    return this;
  }

  @Override
  public final boolean await(final long timeout, final TimeUnit unit) throws InterruptedException {
    return await0(unit.toNanos(timeout), true);
  }

  @Override
  public final QueryFuture awaitUninterruptibly() {
    boolean interrupted = false;
    synchronized (this) {
      while (!done) {
        waiters++;
        try {
          wait();
        } catch (InterruptedException e) {
          interrupted = true;
        } finally {
          waiters--;
        }
      }
    }

    if (interrupted) {
      Thread.currentThread().interrupt();
    }

    return this;
  }

  @Override
  public final boolean awaitUninterruptibly(final long timeout, final TimeUnit unit) {
    try {
      return await0(unit.toNanos(timeout), false);
    } catch (InterruptedException e) {
      throw new InternalError();
    }
  }

  /**
   * Wait the action to be done for at most timeoutNanos nanoseconds.
   * 
   * @param timeoutNanos timeout in nanoseconds
   * @param interruptable true to throw the InterruptedException if interrupted, otherwise just set the interrupted bit.
   * @throws InterruptedException if interrupted and is interruptable.
   * @return if the action is done within timeout.
   */
  private boolean await0(final long timeoutNanos, final boolean interruptable) throws InterruptedException {
    if (interruptable && Thread.interrupted()) {
      throw new InterruptedException();
    }

    long startTime = 0;
    if (timeoutNanos > 0) {
      startTime = System.nanoTime();
    }
    long waitTime = timeoutNanos;
    boolean interrupted = false;

    try {
      synchronized (this) {
        if (done || waitTime <= 0) {
          return done;
        }

        waiters++;
        try {
          for (;;) {
            try {
              long ms = TimeUnit.NANOSECONDS.toMillis(waitTime);
              wait(ms, (int) (waitTime - TimeUnit.MILLISECONDS.toNanos(ms)));
            } catch (InterruptedException e) {
              if (interruptable) {
                throw e;
              } else {
                interrupted = true;
              }
            }

            if (done) {
              return true;
            } else {
              waitTime = timeoutNanos - (System.nanoTime() - startTime);
              if (waitTime <= 0) {
                return done;
              }
            }
          }
        } finally {
          waiters--;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public final boolean setSuccess() {
    synchronized (this) {
      // Allow only once.
      if (done) {
        return false;
      }

      done = true;
      if (waiters > 0) {
        notifyAll();
      }
    }

    notifyListeners();
    return true;
  }

  @Override
  public final boolean setFailure(final Throwable cause) {
    synchronized (this) {
      // Allow only once.
      if (done) {
        return false;
      }

      this.cause = cause;
      done = true;
      if (waiters > 0) {
        notifyAll();
      }
    }

    notifyListeners();
    return true;
  }

  @Override
  public final boolean cancel() {
    if (!cancellable) {
      return false;
    }

    synchronized (this) {
      // Allow only once.
      if (done) {
        return false;
      }

      cause = CANCELLED;
      done = true;
      if (waiters > 0) {
        notifyAll();
      }
    }

    notifyListeners();
    return true;
  }

  /**
   * notify the listeners.
   * */
  private void notifyListeners() {
    // This method doesn't need synchronization because:
    // 1) This method is always called after synchronized (this) block.
    // Hence any listener list modification happens-before this method.
    // 2) This method is called only when 'done' is true. Once 'done'
    // becomes true, the listener list is never modified - see add/removeListener()
    if (firstListener != null) {
      notifyListener(firstListener);
      firstListener = null;

      if (otherListeners != null) {
        for (QueryFutureListener l : otherListeners) {
          notifyListener(l);
        }
        otherListeners = null;
      }
    }
  }

  /**
   * Notify a single listener.
   * 
   * @param l the listener to be notified.
   * */
  private void notifyListener(final QueryFutureListener l) {
    try {
      l.operationComplete(this);
    } catch (Throwable t) {
      if (LOGGER.isWarnEnabled()) {
        LOGGER.warn("An exception was thrown by " + QueryFutureListener.class.getSimpleName() + '.', t);
      }
    }
  }

  @Override
  public final boolean setProgress(final long amount, final long current, final long total) {
    QueryFutureProgressListener[] plisteners;
    synchronized (this) {
      // Do not generate progress event after completion.
      if (done) {
        return false;
      }

      Collection<QueryFutureProgressListener> progressListenersLocal = progressListeners;
      if (progressListenersLocal == null || progressListenersLocal.isEmpty()) {
        // Nothing to notify - no need to create an empty array.
        return true;
      }

      plisteners = progressListenersLocal.toArray(new QueryFutureProgressListener[progressListenersLocal.size()]);
    }

    for (QueryFutureProgressListener pl : plisteners) {
      notifyProgressListener(pl, amount, current, total);
    }

    return true;
  }

  /**
   * Notify progress listeners.
   * 
   * @param l the listener
   * @param amount the amount of progress finished in the most recent operation
   * @param current the current finished amount
   * @param total the total amount to finish
   * 
   * */
  private void notifyProgressListener(final QueryFutureProgressListener l, final long amount, final long current,
      final long total) {

    try {
      l.operationProgressed(this, amount, current, total);
    } catch (Throwable t) {
      if (LOGGER.isWarnEnabled()) {
        LOGGER.warn("An exception was thrown by " + QueryFutureProgressListener.class.getSimpleName() + '.', t);
      }
    }
  }

}
