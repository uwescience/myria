package edu.washington.escience.myriad.parallel;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.util.Attachmentable;
import edu.washington.escience.myriad.util.concurrent.OperationFuture;
import edu.washington.escience.myriad.util.concurrent.OperationFutureListener;

/**
 * The result of an asynchronous {@link QueryPartition} operation.
 */
public interface QueryFuture extends Attachmentable, OperationFuture {

  /**
   * @return the query where the query operation associated with this future takes place.
   */
  QueryPartition getQuery();

  @Override
  QueryFuture addListener(OperationFutureListener listener);

  @Override
  QueryFuture removeListener(final OperationFutureListener listener);

  @Override
  QueryFuture sync() throws InterruptedException, DbException;

  @Override
  QueryFuture syncUninterruptibly() throws DbException;

  @Override
  QueryFuture await() throws InterruptedException;

  @Override
  QueryFuture awaitUninterruptibly();

}
