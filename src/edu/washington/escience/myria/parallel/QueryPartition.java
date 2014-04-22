package edu.washington.escience.myria.parallel;

import java.util.Set;

import edu.washington.escience.myria.MyriaConstants.FTMODE;

/**
 * A {@link QueryPartition} is a partition of a query plan at a single worker or at the master.
 * */
public interface QueryPartition extends Comparable<QueryPartition> {

  /**
   * get the ftMode.
   * 
   * @return the ft mode.
   * */
  FTMODE getFTMode();

  /**
   * @return the profiling mode.
   */
  boolean isProfilingMode();

  /**
   * @return The query ID.
   * */
  long getQueryID();

  /**
   * @return the query priority.
   * */
  int getPriority();

  /**
   * @param priority set query priority
   * */
  void setPriority(final int priority);

  /**
   * Start execution.
   * 
   * @return the query partition execution future;
   * */
  QueryFuture startExecution();

  /**
   * Prepare to execute, reserve resources, allocate data structures to be used in execution, etc.
   * 
   * @return the init operation future
   * */
  QueryFuture init();

  /**
   * Pause the query. If the query is currently paused, do nothing.
   * 
   * @return the future instance of the pause action. The future will be set as done if and only if all the tasks in
   *         this query have stopped execution. During a pause of the query, all call to this method returns the same
   *         future instance. Two pause calls when the query is not paused at either of the calls return two different
   *         instances.
   * */
  QueryFuture pause();

  /**
   * Resume the query.
   * 
   * @return the future instance of the resume action.
   * */
  QueryFuture resume();

  /**
   * Kill the query.
   * 
   * @return the kill operation future;
   * */
  QueryFuture kill();

  /**
   * @return if the query is paused.
   * */
  boolean isPaused();

  /**
   * @return the query execution statistics.
   * */
  QueryExecutionStatistics getExecutionStatistics();

  /**
   * @return the set of workers that are currently dead.
   */
  Set<Integer> getMissingWorkers();

  /**
   * @return the future for the query's execution.
   * */
  QueryFuture getExecutionFuture();
}
