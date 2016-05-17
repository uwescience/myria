/*
 * This file borrows its skeleton from the DefaultChannelFuture in Netty.
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
package edu.washington.escience.myria.util.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.DbException;

/**
 * This class provides default implementations to the {@link OperationFuture}.
 * <p>
 * It defines the following behaviors:
 *
 * <ol>
 * <li>All listeners executed by the order they get added.</li>
 * <li>All completed state listeners are executed by the thread caused the state change.</li>
 * <li>All listeners added after the future entered into a completed state are executed by the thread adding them.</li>
 * <li>States are denoted by an integer. All odd integers denote completed states. And all even integers denote
 * uncompleted states.</li>
 * </ol>
 *
 *
 * @param <T> possible operation result when operation is successfully conducted.
 */
public abstract class OperationFutureBase<T> implements OperationFuture {

  /**
   * logger.
   * */
  private static final Logger LOGGER = LoggerFactory.getLogger(OperationFutureBase.class);

  /**
   * if the future is cancellable.
   * */
  private final boolean cancellable;

  /**
   * The first listener of all the list of listeners.
   * */
  private OperationFutureListener firstListener;

  /**
   * All other listeners.
   * */
  private List<OperationFutureListener> otherListeners;

  /**
   * The first pre-notify listener of all the list of listeners. A pre-notify listener will get executed right after the
   * operation is completed and before the threads who are waiting on this future are wakedup.
   * */
  private OperationFutureListener firstPreListener;

  /**
   * All other pre-notify listeners.
   * */
  private List<OperationFutureListener> otherPreListeners;

  /**
   * Possible operation result.
   * */
  private volatile T result;

  /**
   * If the action fails, what's the cause.
   * */
  private volatile Throwable cause;

  /**
   * Thread safe. Guarded by this.
   * */
  private int waiters;

  /** Initial state. */
  protected static final int READY = 0;
  /** State value representing that the operation is done. */
  protected static final int DONE = 1;
  /** State value representing that the task is cancelled. */
  protected static final int CANCELL = DONE | (1 << 1);
  /** State value representing that the task succeeded. */
  protected static final int SUCCEED = DONE | (1 << 2);
  /** State value representing that the task failed. */
  protected static final int FAIL = DONE | (1 << 3);

  /**
   * the state of the future. Two types of states:
   * <ol>
   * <li>1. Non-done states. These are temp states, can get changed.</li>
   * <li>2. Done states. Once the future enters a done state, the state cannot get changed.</li>
   * </ol>
   * */
  private final AtomicInteger state = new AtomicInteger(READY);

  /**
   * Check if the state is a done state. A done state is a state with the DONE bit set.
   *
   * @param stateV the state to check.
   * @return if the state is a done state.
   * */
  protected final boolean isDoneState(final int stateV) {
    return (stateV & DONE) != 0;
  }

  /**
   * Atomically change state. Note that if current state is in any of done states, the set will constantly fail.
   *
   * @param expected expected current state
   * @param update the new state to set
   * @return if the set succeeds.
   * */
  protected final boolean compareAndSetState(final int expected, final int update) {
    if (isDone()) {
      return false;
    }
    if (this.state.compareAndSet(expected, update)) {
      return true;
    }
    return false;
  }

  /**
   * Wake up all waiters and notify listeners.
   * */
  protected final void wakeupWaitersAndNotifyListeners() {
    notifyPreListeners();
    synchronized (this) {
      // Allow only once.
      if (waiters > 0) {
        notifyAll();
      }
    }
    notifyListeners();
  }

  /**
   * @param doneState a done state
   * @return if the set succeeds.
   * */
  protected final boolean setDoneState(final int doneState) {
    Preconditions.checkArgument(isDoneState(doneState));
    while (true) {
      int oldState = state.get();
      if (!isDoneState(oldState)) {
        if (this.state.compareAndSet(oldState, doneState)) {
          return true;
        }
      } else {
        return false;
      }
    }
  }

  /**
   * Creates a new instance.
   *
   * @param cancellable {@code true} if and only if this future can be canceled
   */
  public OperationFutureBase(final boolean cancellable) {
    this.cancellable = cancellable;
  }

  /**
   * @return if the operation is cancellable.
   * */
  protected final boolean isCancellable() {
    return this.cancellable;
  }

  @Override
  public final boolean isDone() {
    return (this.state.get() & DONE) != 0;
  }

  @Override
  public final boolean isSuccess() {
    return (this.state.get() == SUCCEED);
  }

  @Override
  public final Throwable getCause() {
    return cause;
  }

  @Override
  public final synchronized boolean isCancelled() {
    return this.state.get() == CANCELL;
  }

  /**
   * Adds the specified listener to this future. The specified listener is notified when this future is
   * {@linkplain #isDone() done}. If this future is already completed, the specified listener is notified immediately.
   *
   * @param listener the listener.
   */
  protected final void addListener0(final OperationFutureListener listener) {
    if (listener == null) {
      throw new NullPointerException("listener");
    }

    boolean notifyNow = false;
    synchronized (this) {
      if (isDone()) {
        notifyNow = true;
      } else {
        if (firstListener == null) {
          firstListener = listener;
        } else {
          if (otherListeners == null) {
            otherListeners = new ArrayList<OperationFutureListener>(1);
          }
          otherListeners.add(listener);
        }
      }
    }

    if (notifyNow) {
      notifyListener(listener);
    }
  }

  /**
   * A pre-notify listener will get executed right after the operation is completed and before the threads who are
   * waiting on this future are wakedup. If the future is already completed, the specified listener is notified
   * immediately.
   *
   * Adds the specified listener to this future. If the future is already completed, the specified listener is notified
   * immediately.
   *
   * @param listener the listener.
   * */
  protected final void addPreListener0(final OperationFutureListener listener) {
    if (listener == null) {
      throw new NullPointerException("listener");
    }
    boolean notifyNow = false;
    synchronized (this) {
      if (isDone()) {
        notifyNow = true;
      } else {
        if (firstPreListener == null) {
          firstPreListener = listener;
        } else {
          if (otherPreListeners == null) {
            otherPreListeners = new ArrayList<OperationFutureListener>(1);
          }
          otherPreListeners.add(listener);
        }
      }
    }

    if (notifyNow) {
      notifyListener(listener);
    }
  }

  /**
   * Removes the specified listener from this future. The specified listener is no longer notified when this future is
   * {@linkplain #isDone() done}. If the specified listener is not associated with this future, this method does nothing
   * and returns silently.
   *
   * @param listener the listener to be removed.
   */
  protected final void removeListener0(final OperationFutureListener listener) {
    if (listener == null) {
      throw new NullPointerException("listener");
    }
    if (isDone()) {
      // Do nothing if already done.
      return;
    }
    synchronized (this) {
      if (listener == firstListener) {
        if (otherListeners != null && !otherListeners.isEmpty()) {
          firstListener = otherListeners.remove(0);
        } else {
          firstListener = null;
        }
      } else if (otherListeners != null) {
        otherListeners.remove(listener);
      }
    }
  }

  /**
   * Waits for this future until it is done, and rethrows the cause of the failure if this future failed. If the cause
   * of the failure is a checked exception, it is wrapped with a new {@link DbException} before being thrown.
   *
   * @throws InterruptedException if interrupted.
   * @throws DbException if any other error occurs.
   */
  protected final void sync0() throws InterruptedException, DbException {
    await();
    rethrowIfFailed0();
  }

  /**
   * Waits for this future until it is done, and rethrows the cause of the failure if this future failed. If the cause
   * of the failure is a checked exception, it is wrapped with a new {@link DbException} before being thrown.
   *
   * @throws DbException if any error occurs.
   */
  protected final void syncUninterruptibly0() throws DbException {
    awaitUninterruptibly();
    rethrowIfFailed0();
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

  /**
   * Waits for this future to be completed.
   *
   * @throws InterruptedException if the current thread was interrupted
   */
  protected final void await0() throws InterruptedException {
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }

    synchronized (this) {
      while (!isDone()) {
        waiters++;
        try {
          wait();
        } finally {
          waiters--;
        }
      }
    }
  }

  @Override
  public final boolean await(final long timeout, final TimeUnit unit) throws InterruptedException {
    return await0(unit.toNanos(timeout), true);
  }

  /**
   * Waits for this future to be completed without interruption. This method catches an {@link InterruptedException} and
   * discards it silently.
   *
   */
  protected final void awaitUninterruptibly0() {
    boolean interrupted = false;
    synchronized (this) {
      while (!isDone()) {
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
  private boolean await0(final long timeoutNanos, final boolean interruptable)
      throws InterruptedException {
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
        if (isDone() || waitTime <= 0) {
          return isDone();
        }

        waiters++;
        try {
          for (; ; ) {
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

            if (isDone()) {
              return true;
            } else {
              waitTime = timeoutNanos - (System.nanoTime() - startTime);
              if (waitTime <= 0) {
                return isDone();
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

  /**
   * Marks this future as a success and notifies all listeners.
   *
   * @param result operation result, null if no result is generated
   * @return {@code true} if and only if successfully marked this future as a success. Otherwise {@code false} because
   *         this future is already marked as either a success or a failure.
   */
  protected final boolean setSuccess0(final T result) {
    if (!this.setDoneState(SUCCEED)) {
      return false;
    }
    this.result = result;
    this.wakeupWaitersAndNotifyListeners();
    return true;
  }

  /**
   * @return operation result. Null if the operation doesn't generate result.
   * */
  protected final synchronized T getResult() {
    return this.result;
  }

  /**
   * Marks this future as a failure and notifies all listeners.
   *
   * @param cause the cause.
   * @return {@code true} if and only if successfully marked this future as a failure. Otherwise {@code false} because
   *         this future is already marked as either a success or a failure.
   */
  protected final boolean setFailure0(final Throwable cause) {
    if (!this.setDoneState(FAIL)) {
      return false;
    }
    this.cause = cause;
    this.wakeupWaitersAndNotifyListeners();
    return true;
  }

  @Override
  public final boolean cancel() {
    if (!cancellable) {
      return false;
    }
    if (!this.setDoneState(CANCELL)) {
      return false;
    }
    boolean canceled = doCancel();
    this.wakeupWaitersAndNotifyListeners();
    return canceled;
  }

  /**
   * Do the actual cancel operations.
   *
   * @return true if cancellation is successful.
   * */
  protected boolean doCancel() {
    return true;
  };

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
        for (OperationFutureListener l : otherListeners) {
          notifyListener(l);
        }
        otherListeners = null;
      }
    }
  }

  /**
   * notify the listeners.
   * */
  private void notifyPreListeners() {
    // This method doesn't need synchronization because:
    // 1) This method is always called after synchronized (this) block.
    // Hence any listener list modification happens-before this method.
    // 2) This method is called only when 'done' is true. Once 'done'
    // becomes true, the listener list is never modified - see add/removeListener()
    if (firstPreListener != null) {
      notifyListener(firstPreListener);
      firstPreListener = null;

      if (otherPreListeners != null) {
        for (OperationFutureListener l : otherPreListeners) {
          notifyListener(l);
        }
        otherPreListeners = null;
      }
    }
  }

  /**
   * Notify a single listener.
   *
   * @param l the listener to be notified.
   * */
  private void notifyListener(final OperationFutureListener l) {
    try {
      l.operationComplete(this);
    } catch (Exception t) {
      if (LOGGER.isWarnEnabled()) {
        LOGGER.warn(
            "An exception was thrown by " + OperationFutureListener.class.getSimpleName() + '.', t);
      }
    } catch (Throwable t) {
      LOGGER.info(
          "A throwable was thrown by " + OperationFutureListener.class.getSimpleName() + '.', t);
      throw t;
    }
  }
}
