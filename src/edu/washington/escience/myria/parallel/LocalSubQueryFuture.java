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
package edu.washington.escience.myria.parallel;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.util.concurrent.OperationFutureBase;
import edu.washington.escience.myria.util.concurrent.OperationFutureListener;

/**
 * The result of an asynchronous {@link LocalSubQuery} operation.
 */
class LocalSubQueryFuture extends OperationFutureBase<Void> {

  /**
   * The {@link LocalSubQuery} associated with this future.
   */
  private final LocalSubQuery query;

  /**
   * Creates a new instance.
   *
   * @param query the {@link LocalSubQuery} associated with this future
   * @param cancellable {@code true} if and only if this future can be canceled
   */
  public LocalSubQueryFuture(final LocalSubQuery query, final boolean cancellable) {
    super(cancellable);
    this.query = query;
  }

  /**
   * Returns the {@link LocalSubQuery} associated with this future.
   *
   * @return the {@link LocalSubQuery} associated with this future
   */
  public final LocalSubQuery getLocalSubQuery() {
    return query;
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

  @Override
  public LocalSubQueryFuture addListener(final OperationFutureListener listener) {
    super.addListener0(listener);
    return this;
  }

  @Override
  public LocalSubQueryFuture removeListener(final OperationFutureListener listener) {
    super.removeListener0(listener);
    return this;
  }

  @Override
  public LocalSubQueryFuture sync() throws InterruptedException, DbException {
    super.sync0();
    return this;
  }

  @Override
  public LocalSubQueryFuture syncUninterruptibly() throws DbException {
    super.syncUninterruptibly0();
    return this;
  }

  @Override
  public LocalSubQueryFuture await() throws InterruptedException {
    super.await0();
    return this;
  }

  @Override
  public LocalSubQueryFuture awaitUninterruptibly() {
    super.awaitUninterruptibly0();
    return this;
  }

  @Override
  public LocalSubQueryFuture addPreListener(final OperationFutureListener listener) {
    super.addPreListener0(listener);
    return this;
  }
}
