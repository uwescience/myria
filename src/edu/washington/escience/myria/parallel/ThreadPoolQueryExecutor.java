package edu.washington.escience.myria.parallel;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import edu.washington.escience.myria.util.concurrent.ExecutableExecutionFuture;
import edu.washington.escience.myria.util.concurrent.ExecutionFuture;

/**
 * This query executor implementation executes all the tasks in a shared thread pool. The thread pool size is unlimited.
 * */
public class ThreadPoolQueryExecutor implements QueryExecutor {

  /**
   * Thread pool task executor.
   * */
  private class TPTaskExecutor implements TaskExecutor {

    /**
     * Helper variable for checking the inTaskExecutor.
     * */
    private final ThreadLocal<Boolean> inTaskExecutor = new ThreadLocal<Boolean>();

    @Override
    public boolean inTaskExecutor() {
      return inTaskExecutor.get() != null;
    }

    @Override
    public void release() {
    }

    @Override
    public <T> ExecutionFuture<T> submit(final Callable<T> c) {

      ExecutableExecutionFuture<T> eef = new ExecutableExecutionFuture<T>(new Callable<T>() {
        @Override
        public T call() throws Exception {
          return c.call();
        }
      }, true);

      baseExecutor.submit((Callable<T>) eef);
      return eef;
    }

  }

  /**
   * base executor.
   * */
  private volatile ExecutorService baseExecutor;
  /**
   * thread factory.
   * */
  private final ThreadFactory threadFactory;

  /**
   * @param threadFactory thread factory.
   * */
  public ThreadPoolQueryExecutor(final ThreadFactory threadFactory) {
    this.threadFactory = threadFactory;
  }

  @Override
  public void start() {
    baseExecutor = Executors.newCachedThreadPool(threadFactory);
  }

  @Override
  public void shutdown() {
    if (baseExecutor != null) {
      baseExecutor.shutdown();
    }
  }

  @Override
  public void shutdownNow() {
    if (baseExecutor != null) {
      baseExecutor.shutdownNow();
    }
  }

  @Override
  public TPTaskExecutor nextTaskExecutor(final QuerySubTreeTask task) {
    return new TPTaskExecutor();
  }

}
