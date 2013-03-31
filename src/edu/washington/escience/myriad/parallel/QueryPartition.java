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
   * Call back if a task finishes.
   * 
   * @param task the finished task.
   * */
  void taskFinish(final QuerySubTreeTask task);

  /**
   * Pause the query.
   * */
  void pause();

  /**
   * Resume the query.
   * */
  void resume();

  /**
   * Kill the query.
   * */
  void kill();
}
