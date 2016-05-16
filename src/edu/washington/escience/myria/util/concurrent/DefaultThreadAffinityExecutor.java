package edu.washington.escience.myria.util.concurrent;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

/**
 * Simply wrap a 1-size {@link ThreadPoolExecutor}.
 * */
class DefaultThreadAffinityExecutor implements ThreadAffinityExecutor {

  /**
   * The thread to run the jobs.
   * */
  private volatile ThreadPoolExecutor backendThread;

  /**
   * @param threadFactory thread factory for creating threads in the executor.
   * */
  DefaultThreadAffinityExecutor(final ThreadFactory threadFactory) {
    backendThread =
        new ThreadPoolExecutor(
            1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), threadFactory);
  }

  @Override
  public void execute(final Runnable command) {
    Preconditions.checkNotNull(command);
    this.submit(Executors.callable(command));
  }

  @Override
  public <T> ExecutionFuture<T> submit(final Callable<T> task) {
    Preconditions.checkNotNull(task);
    ExecutableExecutionFuture<T> r = new ExecutableExecutionFuture<T>(task, true);
    backendThread.submit((Callable<T>) r);
    return r;
  }

  /**
   * Normal shutdown, wait until the current running task finishes.
   * */
  void shutdown() {
    backendThread.shutdown();
    backendThread = null;
  }

  /**
   * Abrupt shutdown, interrupt the execution thread.
   *
   * @return list of {@link Runnable}s that never commenced execution.
   * */
  List<Runnable> shutdownNow() {
    try {
      return backendThread.shutdownNow();
    } finally {
      backendThread = null;
    }
  }

  /**
   * Returns <tt>true</tt> if all tasks have completed following shut down. Note that <tt>isTerminated</tt> is never
   * <tt>true</tt> unless either <tt>shutdown</tt> or <tt>shutdownNow</tt> was called first.
   *
   * @return <tt>true</tt> if all tasks have completed following shut down
   */
  boolean isTerminated() {
    return backendThread.isTerminated();
  }

  /**
   * Blocks until all tasks have completed execution after a shutdown request, or the timeout occurs, or the current
   * thread is interrupted, whichever happens first.
   *
   * @param timeout the maximum time to wait
   * @param unit the time unit of the timeout argument
   * @return <tt>true</tt> if this executor terminated and <tt>false</tt> if the timeout elapsed before termination
   * @throws InterruptedException if interrupted while waiting
   */
  boolean awaitTermination(final long timeout, final TimeUnit unit) throws InterruptedException {
    return backendThread.awaitTermination(timeout, unit);
  }
}
