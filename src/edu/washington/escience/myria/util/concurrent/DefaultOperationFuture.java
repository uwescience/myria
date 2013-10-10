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
package edu.washington.escience.myria.util.concurrent;

import edu.washington.escience.myria.DbException;

/**
 * The default {@link OperationFuture} implementation.
 * 
 */
public class DefaultOperationFuture extends OperationFutureBase<Void> {

  /**
   * Creates a new instance.
   * 
   * @param cancellable {@code true} if and only if this future can be canceled
   */
  public DefaultOperationFuture(final boolean cancellable) {
    super(cancellable);
  }

  @Override
  public OperationFuture addListener(final OperationFutureListener listener) {
    super.addListener0(listener);
    return this;
  }

  @Override
  public OperationFuture removeListener(final OperationFutureListener listener) {
    super.removeListener0(listener);
    return this;
  }

  @Override
  public OperationFuture sync() throws InterruptedException, DbException {
    super.sync0();
    return this;
  }

  @Override
  public OperationFuture syncUninterruptibly() throws DbException {
    super.syncUninterruptibly0();
    return this;
  }

  @Override
  public OperationFuture await() throws InterruptedException {
    super.await0();
    return this;
  }

  @Override
  public OperationFuture awaitUninterruptibly() {
    super.awaitUninterruptibly0();
    return this;
  }

  /**
   * Marks this future as a success and notifies all listeners.
   * 
   * @return {@code true} if and only if successfully marked this future as a success. Otherwise {@code false} because
   *         this future is already marked as either a success or a failure.
   */
  public final boolean setSuccess() {
    return super.setSuccess0(null);
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
  public final boolean setProgress(final long amount, final long current, final long total) {
    return super.setProgress0(amount, current, total);
  }

  /**
   * Marks this future as a failure and notifies all listeners.
   * 
   * @param cause the cause.
   * @return {@code true} if and only if successfully marked this future as a failure. Otherwise {@code false} because
   *         this future is already marked as either a success or a failure.
   */
  public final boolean setFailure(final Throwable cause) {
    return super.setFailure0(cause);
  }

}
