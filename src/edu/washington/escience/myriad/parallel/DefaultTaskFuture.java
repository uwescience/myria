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

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.util.Attachmentable;
import edu.washington.escience.myriad.util.concurrent.OperationFutureBase;
import edu.washington.escience.myriad.util.concurrent.OperationFutureListener;
import edu.washington.escience.myriad.util.concurrent.OperationFutureProgressListener;

/**
 * The default {@link QueryFuture} implementation.
 */
class DefaultTaskFuture extends OperationFutureBase<Void> implements Attachmentable, TaskFuture {

  /**
   * The owner query of this future, i.e. the future is for an operation on the query.
   * */
  private final QuerySubTreeTask task;

  /**
   * Creates a new instance.
   * 
   * @param task the {@link QuerySubTreeTask } associated with this future
   * @param cancellable {@code true} if and only if this future can be canceled
   */
  public DefaultTaskFuture(final QuerySubTreeTask task, final boolean cancellable) {
    super(cancellable);
    this.task = task;
  }

  @Override
  public final QuerySubTreeTask getTask() {
    return task;
  }

  /**
   * Marks this future as a success and notifies all listeners.
   * 
   * @return {@code true} if and only if successfully marked this future as a success. Otherwise {@code false} because
   *         this future is already marked as either a success or a failure.
   */
  final boolean setSuccess() {
    return setSuccess0(null);
  }

  /**
   * Marks this future as a failure and notifies all listeners.
   * 
   * @param cause the cause.
   * @return {@code true} if and only if successfully marked this future as a failure. Otherwise {@code false} because
   *         this future is already marked as either a success or a failure.
   */
  final boolean setFailure(final Throwable cause) {
    return setFailure0(cause);
  }

  /**
   * Notifies the progress of the operation to the listeners that implements {@link OperationFutureProgressListener}.
   * Please note that this method will not do anything and return {@code false} if this future is complete already.
   * 
   * @param amount the amount of progress finished between the last call of this method and the current call
   * @param current the current finished amount
   * @param total the total amount to finish
   * @return {@code true} if and only if notification was made.
   */
  final boolean setProgress(final long amount, final long current, final long total) {
    return setProgress0(amount, current, total);
  }

  @Override
  public TaskFuture addListener(final OperationFutureListener listener) {
    super.addListener0(listener);
    return this;
  }

  @Override
  public TaskFuture removeListener(final OperationFutureListener listener) {
    super.removeListener0(listener);
    return this;
  }

  @Override
  public TaskFuture sync() throws InterruptedException, DbException {
    super.sync0();
    return this;
  }

  @Override
  public TaskFuture syncUninterruptibly() throws DbException {
    super.syncUninterruptibly0();
    return this;
  }

  @Override
  public TaskFuture await() throws InterruptedException {
    super.await0();
    return this;
  }

  @Override
  public TaskFuture awaitUninterruptibly() {
    super.awaitUninterruptibly0();
    return this;
  }

}
