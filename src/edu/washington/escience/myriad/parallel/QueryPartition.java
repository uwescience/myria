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
   * Call back if a task : <br>
   * <ul>
   * <li>
   * finishes normally.</li>
   * <li>gets killed by:
   * <ul>
   * <li>kill query command &nbsp;</li>
   * <li>any exceptions</li></li>
   * </ul>
   * </ul>
   * 
   * @param task the finished task.
   * */
  void taskFinish(final QuerySubTreeTask task);

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
   * @return the future instance of the kill action.
   * */
  QueryFuture kill();

  /**
   * @return if the query is paused.
   * */
  boolean isPaused();

  /**
   * @return if the query is killed.
   * */
  boolean isKilled();
}
