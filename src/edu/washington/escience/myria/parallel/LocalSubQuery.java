package edu.washington.escience.myria.parallel;

import java.util.Set;

import edu.washington.escience.myria.MyriaConstants.FTMODE;

/**
 * A {@link LocalSubQuery} is the instantiation of the part of a distributed subquery that executes at one node. It
 * typically contains one or more {@link LocalFragment}s that represent the individual subquery fragments that make up
 * the local component of the subquery.
 */
public interface LocalSubQuery extends Comparable<LocalSubQuery> {

  /**
   * get the ftMode.
   * 
   * @return the ft mode.
   */
  FTMODE getFTMode();

  /**
   * @return the profiling mode.
   */
  boolean isProfilingMode();

  /**
   * @return The (global) ID for this subquery.
   */
  SubQueryId getSubQueryId();

  /**
   * @return the priority of this subquery.
   */
  int getPriority();

  /**
   * @param priority the priority of this subquery
   */
  void setPriority(final int priority);

  /**
   * Start execution.
   */
  void startExecution();

  /**
   * Prepare to execute, reserve resources, allocate data structures to be used in execution, etc.
   */
  void init();

  /**
   * Kill this subquery.
   */
  void kill();

  /**
   * @return the execution statistics for this subquery.
   */
  ExecutionStatistics getExecutionStatistics();

  /**
   * @return the set of workers that are currently dead.
   */
  Set<Integer> getMissingWorkers();

  /**
   * @return the future for the query's execution.
   */
  LocalSubQueryFuture getExecutionFuture();
}
