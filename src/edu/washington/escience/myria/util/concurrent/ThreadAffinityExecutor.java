package edu.washington.escience.myria.util.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

/**
 * An {@link Executor} that confines all the tasks submitted to this executor to the same thread.
 * */
public interface ThreadAffinityExecutor extends Executor {

  /**
   * @param task the task to run in the executor.
   * @param <T> the return type of the task.
   * @return the future to refer to the running state of the task.
   * */
  <T> ExecutionFuture<T> submit(final Callable<T> task);
}
