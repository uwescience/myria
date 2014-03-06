package edu.washington.escience.myria.parallel;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.util.concurrent.ExecutionFuture;
import edu.washington.escience.myria.util.concurrent.ThreadAffinityExecutor;
import edu.washington.escience.myria.util.concurrent.ThreadAffinityExecutorService;
import edu.washington.escience.myria.util.concurrent.ThreadAffinityFixedRoundRobinExecutionPool;

/**
 * This query executor implementation has a fixed number of execution threads. It executes each task in a single thread
 * through out the whole life circle of the task. But different tasks may share the same execution thread.
 * */
public class ThreadAffinityFixedSizeQueryExecutor implements QueryExecutor {

  /**
   * Single thread task executor.
   * */
  private class SingleThreadTaskExecutor implements TaskExecutor {
    /**
     * Base thread affinity executor.
     * */
    private final ThreadAffinityExecutor baseExecutor;
    /**
     * The backend thread in the base executor.
     * */
    private final Thread backendThread;
    /**
     * The executor resource is reserved by the key.
     * */
    private final Runnable key;

    /**
     * @param key The executor resource is reserved by the key.
     * @param baseExecutor base executor.
     * */
    public SingleThreadTaskExecutor(final Runnable key, final ThreadAffinityExecutor baseExecutor) {
      this.baseExecutor = Preconditions.checkNotNull(baseExecutor);
      try {
        backendThread = baseExecutor.submit(new Callable<Thread>() {
          @Override
          public Thread call() throws Exception {
            return Thread.currentThread();
          }
        }).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new IllegalStateException(e);
      }
      this.key = Preconditions.checkNotNull(key);
    }

    /**
     * @return the resource key.
     * */
    public Runnable getKey() {
      return key;
    }

    @Override
    public boolean inTaskExecutor() {
      return Thread.currentThread() == backendThread;
    }

    @Override
    public void release() {
      ThreadAffinityFixedSizeQueryExecutor.this.baseExecutor.clearExecutor(getKey());
    }

    @Override
    public <T> ExecutionFuture<T> submit(final Callable<T> c) {
      return baseExecutor.submit(c);
    }

  }

  /**
   * Base executor.
   * */
  private volatile ThreadAffinityExecutorService baseExecutor;
  /**
   * Number of threads (task slots) in this executor.
   * */
  private final int numThreads;
  /**
   * Thread factory.
   * */
  private final ThreadFactory threadFactory;

  /**
   * @param numTaskSlots number of task slots (threads)
   * @param threadFactory thread factory
   * */
  public ThreadAffinityFixedSizeQueryExecutor(final int numTaskSlots, final ThreadFactory threadFactory) {
    Preconditions.checkArgument(numTaskSlots > 0);
    numThreads = numTaskSlots;
    this.threadFactory = threadFactory;
  }

  @Override
  public void start() {
    baseExecutor = new ThreadAffinityFixedRoundRobinExecutionPool(numThreads, threadFactory);
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
  public SingleThreadTaskExecutor nextTaskExecutor(final QuerySubTreeTask task) {
    Runnable key = new Runnable() {
      @Override
      public void run() {
      }
    };
    return new SingleThreadTaskExecutor(key, baseExecutor.getExecutor(key));
  }

}
