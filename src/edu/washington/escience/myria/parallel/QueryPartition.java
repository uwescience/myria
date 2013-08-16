package edu.washington.escience.myria.parallel;

import java.util.Set;

/**
 * A {@link QueryPartition} is a partition of a query plan at a single worker or at the master.
 * */
public interface QueryPartition extends Comparable<QueryPartition> {

  /**
   * get the ftMode.
   * 
   * @return the ft mode.
   * */
  String getFTMode();

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
   * */
  void startExecution();

  /**
   * Prepare to execute, reserve resources, allocate data structures to be used in execution, etc.
   * */
  void init();

  /**
   * Pause the query.
   * 
   * @return the future instance of the pause action.
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
   * */
  void kill();

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
}
