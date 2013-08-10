/*
 * 
 * This file is a modification to the ChannelFuture interface in Netty.
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

import java.util.concurrent.TimeUnit;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.util.Attachmentable;

/**
 * The result of an asynchronous {@link QueryPartition} operation.
 * <p>
 * All query operations in Myria are asynchronous. It means any query related operations will return immediately with no
 * guarantee that the requested operation has been completed at the end of the call. Instead, you will be returned with
 * a {@link QueryFuture} instance which gives you the information about the result or status of the query operation.
 * <p>
 * A {@link QueryFuture} is either <em>uncompleted</em> or <em>completed</em>. When an query operation begins, a new
 * future object is created. The new future is uncompleted initially - it is neither succeeded, failed, nor cancelled
 * because the query operation is not finished yet. If the query operation is finished either successfully, with
 * failure, or by cancellation, the future is marked as completed with more specific information, such as the cause of
 * the failure. Please note that even failure and cancellation belong to the completed state.
 * 
 * <pre>
 *                                      +---------------------------+
 *                                      | Completed successfully    |
 *                                      +---------------------------+
 *                                 +---->      isDone() = <b>true</b>      |
 * +--------------------------+    |    |   isSuccess() = <b>true</b>      |
 * |        Uncompleted       |    |    +===========================+
 * +--------------------------+    |    | Completed with failure    |
 * |      isDone() = <b>false</b>    |    |    +---------------------------+
 * |   isSuccess() = false    |----+---->   isDone() = <b>true</b>         |
 * | isCancelled() = false    |    |    | getCause() = <b>non-null</b>     |
 * |    getCause() = null     |    |    +===========================+
 * +--------------------------+    |    | Completed by cancellation |
 *                                 |    +---------------------------+
 *                                 +---->      isDone() = <b>true</b>      |
 *                                      | isCancelled() = <b>true</b>      |
 *                                      +---------------------------+
 * </pre>
 * 
 * Various methods are provided to let you check if the query operation has been completed, wait for the completion, and
 * retrieve the result of the query operation. It also allows you to add {@link QueryFutureListener}s so you can get
 * notified when the query operation is completed.
 * 
 * <h3>Prefer {@link #addListener(QueryFutureListener)} to {@link #await()}</h3>
 * 
 * It is recommended to prefer {@link #addListener(QueryFutureListener)} to {@link #await()} wherever possible to get
 * notified when an query operation is done and to do any follow-up tasks.
 * <p>
 * {@link #addListener(QueryFutureListener)} is non-blocking. It simply adds the specified {@link QueryFutureListener}
 * to the {@link QueryFuture}, and query thread will notify the listeners when the query operation associated with the
 * future is done. {@link QueryFutureListener} yields the best performance and resource utilization because it does not
 * block at all, but it could be tricky to implement a sequential logic if you are not used to event-driven programming.
 * <p>
 * By contrast, {@link #await()} is a blocking operation. Once called, the caller thread blocks until the operation is
 * done. It is easier to implement a sequential logic with {@link #await()}, but the caller thread blocks unnecessarily
 * until the query operation is done and there's relatively expensive cost of inter-thread notification. Moreover,
 * there's a chance of dead lock in a particular circumstance, which is described below.
 * <p>
 * In spite of the disadvantages mentioned above, there are certainly the cases where it is more convenient to call
 * {@link #await()}. In such a case, please make sure you do not call {@link #await()} in an query thread. Otherwise,
 * {@link IllegalStateException} will be raised to prevent a dead lock.
 */
public interface QueryFuture extends Attachmentable {

  /**
   * @return the query where the query operation associated with this future takes place.
   */
  QueryPartition getQuery();

  /**
   * @return {@code true} if and only if this future is complete, regardless of whether the operation was successful,
   *         failed, or cancelled.
   */
  boolean isDone();

  /**
   * @return {@code true} if and only if this future was cancelled by a {@link #cancel()} method.
   */
  boolean isCancelled();

  /**
   * @return {@code true} if and only if the query operation was completed successfully.
   */
  boolean isSuccess();

  /**
   * Returns the cause of the failed query operation if the query operation has failed.
   * 
   * @return the cause of the failure. {@code null} if succeeded or this future is not completed yet.
   */
  Throwable getCause();

  /**
   * Cancels the query operation associated with this future and notifies all listeners if canceled successfully.
   * 
   * @return {@code true} if and only if the operation has been canceled. {@code false} if the operation can't be
   *         canceled or is already completed.
   */
  boolean cancel();

  /**
   * Marks this future as a success and notifies all listeners.
   * 
   * @return {@code true} if and only if successfully marked this future as a success. Otherwise {@code false} because
   *         this future is already marked as either a success or a failure.
   */
  boolean setSuccess();

  /**
   * Marks this future as a failure and notifies all listeners.
   * 
   * @param cause the cause.
   * @return {@code true} if and only if successfully marked this future as a failure. Otherwise {@code false} because
   *         this future is already marked as either a success or a failure.
   */
  boolean setFailure(Throwable cause);

  /**
   * Notifies the progress of the operation to the listeners that implements {@link QueryFutureProgressListener}. Please
   * note that this method will not do anything and return {@code false} if this future is complete already.
   * 
   * @param amount the amount of progress finished in the most recent operation
   * @param current the total current amount finished
   * @param total the total amount to finish
   * @return {@code true} if and only if notification was made.
   */
  boolean setProgress(final long amount, final long current, final long total);

  /**
   * Adds the specified listener to this future. The specified listener is notified when this future is
   * {@linkplain #isDone() done}. If this future is already completed, the specified listener is notified immediately.
   * 
   * @param listener the listener.
   * @return this {@link QueryFuture}
   */
  QueryFuture addListener(QueryFutureListener listener);

  /**
   * Removes the specified listener from this future. The specified listener is no longer notified when this future is
   * {@linkplain #isDone() done}. If the specified listener is not associated with this future, this method does nothing
   * and returns silently.
   * 
   * @param listener the listener to be removed.
   * @return this {@link QueryFuture}
   */
  QueryFuture removeListener(final QueryFutureListener listener);

  /**
   * Waits for this future until it is done, and rethrows the cause of the failure if this future failed. If the cause
   * of the failure is a checked exception, it is wrapped with a new {@link DbException} before being thrown.
   * 
   * @throws InterruptedException if interrupted.
   * @throws DbException if any other error occurs.
   * @return this {@link QueryFuture}.
   */
  QueryFuture sync() throws InterruptedException, DbException;

  /**
   * Waits for this future until it is done, and rethrows the cause of the failure if this future failed. If the cause
   * of the failure is a checked exception, it is wrapped with a new {@link DbException} before being thrown.
   * 
   * @return this {@link QueryFuture}.
   * @throws DbException if any error occurs.
   */
  QueryFuture syncUninterruptibly() throws DbException;

  /**
   * Waits for this future to be completed.
   * 
   * @throws InterruptedException if the current thread was interrupted
   * @return this {@link QueryFuture}.
   */
  QueryFuture await() throws InterruptedException;

  /**
   * Waits for this future to be completed without interruption. This method catches an {@link InterruptedException} and
   * discards it silently.
   * 
   * @return this {@link QueryFuture}.
   */
  QueryFuture awaitUninterruptibly();

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
