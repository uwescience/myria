package edu.washington.escience.myria.parallel;

import java.util.LinkedList;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.common.base.Verify;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.api.encoding.QueryStatusEncoding;
import edu.washington.escience.myria.api.encoding.QueryStatusEncoding.Status;
import edu.washington.escience.myria.parallel.meta.MetaTask;

/**
 * Keeps track of all the queries that are currently executing on the Server.
 */
public final class QueryState {
  /** The id of this query. */
  private final long queryId;
  /** The id of the next subquery to be issued. */
  private long subqueryId;
  /** The status of the query. Must be kept synchronized. */
  private Status status;
  /** The query tasks to be executed. */
  private final LinkedList<QueryTask> taskQ;
  /** The meta tasks to be executed. */
  private final LinkedList<MetaTask> metaQ;
  /** The execution statistics about this query. */
  private final QueryExecutionStatistics executionStats;
  /** The currently-executing task. */
  private QueryTask currentTask;
  /** The server. */
  private final Server server;
  /** The message explaining why a query failed. */
  private String message;
  /** The future for this query. */
  private final FullQueryFuture future;

  /**
   * Construct a new QueryState object for this query.
   * 
   * @param queryId the id of this query
   * @param plan the execution plan
   * @param server the server on which this query will be executed
   */
  public QueryState(final long queryId, final MetaTask plan, final Server server) {
    this.server = Objects.requireNonNull(server, "server");
    this.queryId = queryId;
    subqueryId = 0;
    status = Status.ACCEPTED;
    executionStats = new QueryExecutionStatistics();
    taskQ = new LinkedList<>();
    metaQ = new LinkedList<>();
    metaQ.add(plan);
    message = null;
    future = FullQueryFuture.create(queryId);
  }

  /**
   * Return the id of this query.
   * 
   * @return the id of this query
   */
  public long getQueryId() {
    return queryId;
  }

  /**
   * Return the status of this query.
   * 
   * @return the status of this query.
   */
  public QueryStatusEncoding.Status getStatus() {
    synchronized (status) {
      return status;
    }
  }

  /**
   * Returns <code>true</code> if this query has finished, and <code>false</code> otherwise.
   * 
   * @return <code>true</code> if this query has finished, and <code>false</code> otherwise
   */
  public boolean isDone() {
    return taskQ.isEmpty() && metaQ.isEmpty();
  }

  /**
   * Returns the task in this query that is currently executing, or null if no task is running.
   * 
   * @return the task in this query that is currently executing, or null if no task is running
   */
  public QueryTask getCurrentTask() {
    return currentTask;
  }

  /**
   * Generates and returns the next task to run.
   * 
   * @return the next task to run
   * @throws DbException if there is an error
   */
  public QueryTask nextTask() throws DbException {
    Preconditions.checkState(currentTask == null, "must call finishTask before calling nextTask");
    if (isDone()) {
      return null;
    }
    if (!taskQ.isEmpty()) {
      currentTask = taskQ.removeFirst();
      currentTask.setTaskId(new QueryTaskId(queryId, subqueryId));
      ++subqueryId;
      return currentTask;
    }
    metaQ.getFirst().instantiate(metaQ, taskQ, server);
    /*
     * The above line may have emptied metaQ, mucked with taskQ, not sure. So just recurse to make sure we do the right
     * thing.
     */
    return nextTask();
  }

  /**
   * Mark the current task as finished.
   */
  public void finishTask() {
    currentTask = null;
  }

  /**
   * Returns the time this query started, in ISO8601 format, or null if the query has not yet been started.
   * 
   * @return the time this query started, in ISO8601 format, or null if the query has not yet been started
   */
  public String getStartTime() {
    return executionStats.getStartTime();
  }

  /**
   * Set the time this query started to now in ISO8601 format.
   */
  public void markStart() {
    executionStats.markQueryStart();
  }

  /**
   * Set this query as having failed due to the specified cause.
   * 
   * @param cause the reason the query failed.
   */
  public void markFailed(final Throwable cause) {
    Preconditions.checkNotNull(cause, "cause");
    markEnd();
    status = Status.ERROR;
    message = cause.getMessage();
    future.setException(cause);
  }

  /**
   * Set the time this query ended to now in ISO8601 format.
   */
  public void markSuccess() {
    markEnd();
    status = Status.SUCCESS;
    future.set(this);
  }

  /**
   * Set the time this query ended to now in ISO8601 format.
   */
  private void markEnd() {
    Verify.verify(currentTask == null, "expect current task to be null when query ends");
    executionStats.markQueryEnd();
  }

  /**
   * Returns the time this query ended, in ISO8601 format, or null if the query has not yet ended.
   * 
   * @return the time this query ended, in ISO8601 format, or null if the query has not yet ended
   */
  public String getEndTime() {
    return executionStats.getEndTime();
  }

  /**
   * Returns the time elapsed (in nanoseconds) since the query started.
   * 
   * @return the time elapsed (in nanoseconds) since the query started
   */
  public Long getElapsedTime() {
    return executionStats.getQueryExecutionElapse();
  }

  /**
   * Return a message explaining why a query failed, or <code>null</code> if the query did not fail.
   * 
   * @return a message explaining why a query failed, or <code>null</code> if the query did not fail
   */
  public String getMessage() {
    return message;
  }

  /**
   * Returns the future on the execution of this query.
   * 
   * @return the future on the execution of this query
   */
  public FullQueryFuture getFuture() {
    return future;
  }

  /**
   * Call when the query has been killed.
   */
  public void markKilled() {
    markEnd();
    status = Status.KILLED;
    future.cancel(true);
  }
}
