/*
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
package edu.washington.escience.myria.parallel;

import edu.washington.escience.myria.util.concurrent.OperationFuture;
import edu.washington.escience.myria.util.concurrent.OperationFutureListener;

/**
 * Listens to the result of a {@link TaskFuture}. The result of the asynchronous {@link Channel} query operation is
 * notified once this listener is added by calling {@link TaskFuture#addListener(TaskFutureListener)}.
 * 
 * <h3>Return the control to the caller quickly</h3>
 * 
 * {@link #operationComplete(TaskFuture)} is directly called by an query thread. Therefore, performing a time consuming
 * task or a blocking operation in the handler method can cause an unexpected pause during query. If you need to perform
 * a blocking operation on query completion, try to execute the operation in a different thread using a thread pool.
 */
public abstract class TaskFutureListener implements OperationFutureListener {

  /**
   * Invoked when the query operation associated with the {@link TaskFuture} has been completed.
   * 
   * @param future the source {@link TaskFuture} which called this callback
   * @throws Exception if any error occurs. But note that any uncaught exception caused by this method will be discarded
   *           silently. Do the exception handling in the code by yourself.
   */
  public abstract void operationComplete(TaskFuture future) throws Exception;

  @Override
  public final void operationComplete(final OperationFuture future) throws Exception {
    this.operationComplete((TaskFuture) future);
  }

}
