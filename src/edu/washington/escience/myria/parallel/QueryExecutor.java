package edu.washington.escience.myria.parallel;

/**
 * This query executor implementation executes each task in its own thread. But different tasks may get executed in the
 * same thread.
 * */
public interface QueryExecutor {

  /**
   * Start the query executor.
   * */
  void start();

  /**
   * shutdown the query executor without interrupting currently running queries.
   * */
  void shutdown();

  /**
   * shutdown the query executor and interrupt currently running queries.
   * */
  void shutdownNow();

  /**
   * @param task the task to get executed by the returned executor.
   * @return next task executor.
   * */
  TaskExecutor nextTaskExecutor(final QuerySubTreeTask task);
}
