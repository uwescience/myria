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
 * The future for a {@link LocalFragment}.
 */
final class LocalFragmentFuture extends OperationFutureBase<Void> {

  /**
   * The {@link LocalFragment} associated with this future.
   */
  private final LocalFragment fragment;

  /**
   * Creates a new instance.
   *
   * @param fragment the {@link LocalFragment} associated with this future
   * @param cancellable {@code true} if and only if this future can be canceled
   */
  public LocalFragmentFuture(final LocalFragment fragment, final boolean cancellable) {
    super(cancellable);
    this.fragment = fragment;
  }

  /**
   * Return the {@link LocalFragment} that this future is associated with.
   *
   * @return the {@link LocalFragment} that this future is associated with
   */
  public LocalFragment getFragment() {
    return fragment;
  }

  /**
   * Marks this future as a success and notifies all listeners.
   *
   * @return {@code true} if and only if successfully marked this future as a success. Otherwise {@code false} because
   *         this future is already marked as either a success or a failure.
   */
  boolean setSuccess() {
    return setSuccess0(null);
  }

  /**
   * Marks this future as a failure and notifies all listeners.
   *
   * @param cause the cause.
   * @return {@code true} if and only if successfully marked this future as a failure. Otherwise {@code false} because
   *         this future is already marked as either a success or a failure.
   */
  boolean setFailure(final Throwable cause) {
    return setFailure0(cause);
  }

  @Override
  public LocalFragmentFuture addListener(final OperationFutureListener listener) {
    super.addListener0(listener);
    return this;
  }

  @Override
  public LocalFragmentFuture removeListener(final OperationFutureListener listener) {
    super.removeListener0(listener);
    return this;
  }

  @Override
  public LocalFragmentFuture sync() throws InterruptedException, DbException {
    super.sync0();
    return this;
  }

  @Override
  public LocalFragmentFuture syncUninterruptibly() throws DbException {
    super.syncUninterruptibly0();
    return this;
  }

  @Override
  public LocalFragmentFuture await() throws InterruptedException {
    super.await0();
    return this;
  }

  @Override
  public LocalFragmentFuture awaitUninterruptibly() {
    super.awaitUninterruptibly0();
    return this;
  }

  @Override
  public LocalFragmentFuture addPreListener(final OperationFutureListener listener) {
    super.addPreListener0(listener);
    return this;
  }
}
