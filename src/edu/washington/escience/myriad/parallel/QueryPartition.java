package edu.washington.escience.myriad.parallel;

/**
 * A {@link QueryPartition} is a partition of a query plan at a single worker or at the master.
 * */
public interface QueryPartition extends Comparable<QueryPartition> {

  /**
   * @return The query ID.
   * */
  long getQueryID();

  /**
   * @return the number of tasks which are already EOS.
   * */
  int getNumTaskEOS();

  /**
   * @return the query priority.
   * */
  int getPriority();

  /**
   * @param priority set query priority
   * */
  void setPriority(final int priority);

  /**
   * Start non-blocking execution.
   * */
  void startNonBlockingExecution();

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
   * @return if the query is killed.
   * */
  boolean isKilled();
}
