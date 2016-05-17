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
package edu.washington.escience.myria.util.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import edu.washington.escience.myria.DbException;

/**
 * The handle of an asynchronous {@link Callable} operation.
 *
 * @param <T> the return type of the {@link Callable}
 */
public interface ExecutionFuture<T> extends Future<T>, OperationFuture {

  /**
   * @return the {@link Callable} object associated with this future.
   */
  Callable<T> getCallable();

  @Override
  ExecutionFuture<T> addListener(OperationFutureListener listener);

  @Override
  ExecutionFuture<T> addPreListener(OperationFutureListener listener);

  @Override
  ExecutionFuture<T> removeListener(final OperationFutureListener listener);

  @Override
  ExecutionFuture<T> sync() throws InterruptedException, DbException;

  @Override
  ExecutionFuture<T> syncUninterruptibly() throws DbException;

  @Override
  ExecutionFuture<T> await() throws InterruptedException;

  @Override
  ExecutionFuture<T> awaitUninterruptibly();
}
