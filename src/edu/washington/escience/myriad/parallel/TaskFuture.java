package edu.washington.escience.myriad.parallel;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.util.Attachmentable;
import edu.washington.escience.myriad.util.concurrent.OperationFuture;
import edu.washington.escience.myriad.util.concurrent.OperationFutureListener;

/**
 * The result of an asynchronous {@link QuerySubTreeTask} operation.
 */
public interface TaskFuture extends Attachmentable, OperationFuture {

  /**
   * @return the query where the query operation associated with this future takes place.
   */
  QuerySubTreeTask getTask();

  @Override
  TaskFuture addListener(OperationFutureListener listener);

  @Override
  TaskFuture removeListener(final OperationFutureListener listener);

  @Override
  TaskFuture sync() throws InterruptedException, DbException;

  @Override
  TaskFuture syncUninterruptibly() throws DbException;

  @Override
  TaskFuture await() throws InterruptedException;

  @Override
  TaskFuture awaitUninterruptibly();

}
