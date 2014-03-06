package edu.washington.escience.myria.parallel;

import java.util.concurrent.Callable;

import edu.washington.escience.myria.util.concurrent.ExecutionFuture;

/**
 * Executor for executing tasks. The code snippets which are submitted through the {@code submit} function are executed
 * sequentially.
 * */
public interface TaskExecutor {

  /**
   * @return if the caller thread is in the task executor.
   * */
  boolean inTaskExecutor();

  /**
   * @param c code to execute
   * @param <T> return value
   * @return execution future
   * */
  <T> ExecutionFuture<T> submit(final Callable<T> c);

  /**
   * Release the resource of this task executor.
   * */
  void release();

}
