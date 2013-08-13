/*
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
package edu.washington.escience.myriad.util.concurrent;

import edu.washington.escience.myriad.parallel.QueryFuture;

/**
 * Listens to the progress of a time-consuming I/O operation such as a large file transfer. If this listener is added to
 * a {@link OperationFuture} of an I/O operation that supports progress notification, the listener's
 * {@link #operationProgressed(OperationFuture, long, long, long)} method will be called back by an I/O thread. If the
 * operation does not support progress notification, {@link #operationProgressed(OperationFuture, long, long, long)}
 * will not be invoked. Like a usual {@link OperationFutureListener} that this interface extends,
 * {@link #operationComplete(OperationFuture)} will be called when the future is marked as complete.
 * 
 * <h3>Return the control to the caller quickly</h3>
 * 
 * {@link #operationProgressed(OperationFuture, long, long, long)} and {@link #operationComplete(OperationFuture)} is
 * directly called by an I/O thread. Therefore, performing a time consuming task or a blocking operation in the handler
 * method can cause an unexpected pause during I/O. If you need to perform a blocking operation on I/O completion, try
 * to execute the operation in a different thread using a thread pool.
 * 
 */
public interface OperationFutureProgressListener extends OperationFutureListener {

  /**
   * Invoked when the I/O operation associated with the {@link QueryFuture} has been progressed.
   * 
   * @param future the source {@link QueryFuture} which called this callback
   * @param amount the amount of work completed by the most recent action.
   * @param current the total amount of work currently completed
   * @param total the total amount of work need to be completed
   * @throws Exception if any error occurs
   * @param <T> the actual future type.
   */
  <T extends OperationFuture> void operationProgressed(T future, long amount, long current, long total)
      throws Exception;
}
