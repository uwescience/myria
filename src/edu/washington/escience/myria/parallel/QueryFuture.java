package edu.washington.escience.myria.parallel;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nullable;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * The future for a query.
 */
public final class QueryFuture implements ListenableFuture<Query> {

  /** The id of the query this future represents. */
  private final long queryId;
  /** The wrapped future, which actually implements all the future behavior. */
  private final ListenableFuture<Query> future;

  /**
   * Create a future for the specified query.
   *
   * @param queryId the id of the query
   * @return a future for the specified query
   */
  public static QueryFuture create(final long queryId) {
    return new QueryFuture(queryId);
  }

  /**
   * The future for a submitted query.
   *
   * @param queryId the id of the submitted query.
   */
  private QueryFuture(final long queryId) {
    this(queryId, SettableFuture.<Query>create());
  }

  /**
   * The future for a submitted query.
   *
   * @param queryId the id of the submitted query.
   * @param future the future to be wrapped.
   */
  private QueryFuture(final long queryId, final ListenableFuture<Query> future) {
    this.queryId = queryId;
    this.future = future;
  }

  /**
   * @return the id of this query
   */
  public long getQueryId() {
    return queryId;
  }

  @Override
  public boolean cancel(final boolean mayInterruptIfRunning) {
    return future.cancel(mayInterruptIfRunning);
  }

  @Override
  public boolean isCancelled() {
    return future.isCancelled();
  }

  @Override
  public boolean isDone() {
    return future.isDone();
  }

  @Override
  public Query get() throws InterruptedException, ExecutionException {
    return future.get();
  }

  @Override
  public Query get(final long timeout, final TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return future.get(timeout, unit);
  }

  @Override
  public void addListener(final Runnable listener, final Executor executor) {
    future.addListener(listener, executor);
  }

  /**
   * Sets the value of this future. This method will return {@code true} if the value was successfully set, or
   * {@code false} if the future has already been set or cancelled.
   *
   * @param value the value the future should hold.
   * @return true if the value was successfully set.
   */
  public boolean set(@Nullable final Query value) {
    if (future instanceof SettableFuture) {
      return ((SettableFuture<Query>) future).set(value);
    }
    return false;
  }

  /**
   * Sets the future to having failed with the given exception. This exception will be wrapped in an
   * {@code ExecutionException} and thrown from the {@code get} methods. This method will return {@code true} if the
   * exception was successfully set, or {@code false} if the future has already been set or cancelled.
   *
   * @param throwable the exception the future should hold.
   * @return true if the exception was successfully set.
   */
  public boolean setException(final Throwable throwable) {
    if (future instanceof SettableFuture) {
      return ((SettableFuture<Query>) future).setException(throwable);
    }
    return false;
  }
}
