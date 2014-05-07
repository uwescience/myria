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
   * @return The query/subquery task ID.
   * */
  QueryTaskId getTaskId();

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
   * Kill the query.
   * 
   * */
  void kill();

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
