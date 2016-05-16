/*
 * This class is a modified version of ChannelFuture in Netty.
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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import edu.washington.escience.myria.DbException;

/**
 * This class serves as the root of all the handle objects for asynchronous operations.
 * <p>
 * An asynchronous operation is an operation which is issued by one thread and then gets executed/served by, typically,
 * another execution entity. The execution entity here can be a thread. For example in {@link ExecutorService}, a
 * {@link Callable} or a {@link Runnable} is submitted by a client thread to an executor pool, in which a thread will
 * execute the code some time later. The execution entity can also be a group of threads, a group of processes, or even
 * a group of computers. For example in a parallel computing system, a job is submitted by a client and gets executed on
 * all the machines governed by the computing system.
 *
 * <h3>State machine</h3>
 * <p>
 * A {@link OperationFuture} maintains a state machine. The state machine has two different groups of states:
 * <em>uncompleted</em> or <em>completed (or done)</em>. The completed state group includes three predefined states:
 * <em>succeed</em>, <em>cancelled</em>, and <em>failed</em>. Implementation classes may add more completed states if
 * necessary. Once an {@link OperationFuture} enters into a completed state, the state will never get changed.
 *
 * <p>
 * When an operation begins, a new {@link OperationFuture} object is created. The new future is uncompleted initially -
 * it is neither succeeded, failed, nor cancelled because the operation is not finished yet. If the operation is
 * finished either successfully, with failure, or by cancellation, the future is marked as completed with more specific
 * information, such as the cause of the failure. Please note that even failure and cancellation belong to the completed
 * state.
 *
 *
 * <pre>
 *                                      +---------------------------+
 *                                      | Completed successfully    |
 *                                      +---------------------------+
 *                                 +---->      isDone() = true      |
 * +--------------------------+    |    |   isSuccess() = true      |
 * |        Uncompleted       |    |    +===========================+
 * +--------------------------+    |    | Completed with failure    |
 * |      isDone() = false    |    |    +---------------------------+
 * |   isSuccess() = false    |----+---->   isDone() = true         |
 * | isCancelled() = false    |    |    | getCause() = non-null     |
 * |    getCause() = null     |    |    +===========================+
 * +--------------------------+    |    | Completed by cancellation |
 *                                 |    +---------------------------+
 *                                 +---->      isDone() = true      |
 *                                      | isCancelled() = true      |
 *                                      +---------------------------+
 * </pre>
 *
 *
 * Various methods are provided to let you check if the operation has been completed, wait for the completion, and
 * retrieve the result of the async operation. It also allows you to add {@link OperationFutureListener}s so you can get
 * notified when the state of the operation is changed.
 *
 * <p>
 *
 * There are two predefined types of listeners: <b>Completed State Listeners</b> and <b>Progress Listeners</b>. The
 * completed state listeners will be notified after the future enters any of the completed state. These listeners will
 * only be executed once. The progress listeners may get notified zero or more times. The semantic of the progress
 * listeners are defined by operation future implementations.
 *
 * <h3>Thread model</h3>
 *
 * The operation task submission thread is typically different from the operation execution thread. But they can be the
 * same.
 *
 * <p>
 * The state of the future object may get modified by one or more threads (for non-completed states). If a thread is
 * trying to modified an already-in-completed-state future, it must fail.
 *
 * If a thread moves the state of the operation into a completed one, all completed state listeners must be notified
 * once and only once. The order is not defined. The thread executing them should be either the one caused the completed
 * state change or a dedicated listener notifier thread.
 * <p>
 *
 * The complete state of the future won't get lost. If a listener is added after the future reached a completed state,
 * the listener should be executed immediately either by the thread adding the listener or by the dedicated listener
 * notifier thread.
 *
 * */
public interface OperationFuture {

  /**
   * Returns <tt>true</tt> if this task completed.
   *
   * Completion may be due to normal termination, an exception, or cancellation -- in all of these cases, this method
   * will return <tt>true</tt>.
   *
   * @return <tt>true</tt> if this task completed
   */
  boolean isDone();

  /**
   * @return {@code true} if and only if the operation was completed successfully.
   */
  boolean isSuccess();

  /**
   * Returns the cause of the failed operation if the operation has failed.
   *
   * @return the cause of the failure. {@code null} if succeeded or this future is not completed yet.
   */
  Throwable getCause();

  /**
   * @return {@code true} if and only if this future was cancelled by a {@link #cancel()} method.
   */
  boolean isCancelled();

  /**
   * Cancels the operation associated with this future and notifies all listeners if canceled successfully.
   *
   * @return {@code true} if and only if the operation has been canceled. {@code false} if the operation can't be
   *         canceled or is already completed.
   */
  boolean cancel();

  /**
   * Adds the specified listener to this future. The specified listener is notified when this future is
   * {@linkplain #isDone() done}. If this future is already completed, the specified listener is notified immediately.
   *
   * @param listener the listener.
   * @return this {@link OperationFuture}
   */
  OperationFuture addListener(OperationFutureListener listener);

  /**
   * A pre-notify listener will get executed right after the operation is completed and before the threads who are
   * waiting on this future are wakedup. If the future is already completed, the specified listener is notified
   * immediately.
   *
   * Adds the specified listener to this future. If the future is already completed, the specified listener is notified
   * immediately.
   *
   * @param listener the listener.
   * @return this {@link OperationFuture}
   * */
  OperationFuture addPreListener(OperationFutureListener listener);

  /**
   * Removes the specified listener from this future. The specified listener is no longer notified when this future is
   * {@linkplain #isDone() done}. If the specified listener is not associated with this future, this method does nothing
   * and returns silently.
   *
   * @param listener the listener to be removed.
   * @return this {@link OperationFuture}
   */
  OperationFuture removeListener(final OperationFutureListener listener);

  /**
   * Waits for this future until it is done, and rethrows the cause of the failure if this future failed. If the cause
   * of the failure is a checked exception, it is wrapped with a new {@link DbException} before being thrown.
   *
   * @throws InterruptedException if interrupted.
   * @throws DbException if any other error occurs.
   * @return this {@link OperationFuture}.
   */
  OperationFuture sync() throws InterruptedException, DbException;

  /**
   * Waits for this future until it is done, and rethrows the cause of the failure if this future failed. If the cause
   * of the failure is a checked exception, it is wrapped with a new {@link DbException} before being thrown.
   *
   * @return this {@link OperationFuture}.
   * @throws DbException if any error occurs.
   */
  OperationFuture syncUninterruptibly() throws DbException;

  /**
   * Waits for this future to be completed.
   *
   * @throws InterruptedException if the current thread was interrupted
   * @return this {@link OperationFuture}.
   */
  OperationFuture await() throws InterruptedException;

  /**
   * Waits for this future to be completed without interruption. This method catches an {@link InterruptedException} and
   * discards it silently.
   *
   * @return this {@link OperationFuture}.
   */
  OperationFuture awaitUninterruptibly();

  /**
   * Waits for this future to be completed within the specified time limit.
   *
   * @param timeout the time out.
   * @param unit time unit of timeout
   * @return {@code true} if and only if the future was completed within the specified time limit
   *
   * @throws InterruptedException if the current thread was interrupted
   */
  boolean await(long timeout, TimeUnit unit) throws InterruptedException;

  /**
   * Waits for this future to be completed within the specified time limit without interruption. This method catches an
   * {@link InterruptedException} and discards it silently.
   *
   * @param timeout the time out.
   * @param unit time unit of timeout
   * @return {@code true} if and only if the future was completed within the specified time limit
   */
  boolean awaitUninterruptibly(long timeout, TimeUnit unit);
}
