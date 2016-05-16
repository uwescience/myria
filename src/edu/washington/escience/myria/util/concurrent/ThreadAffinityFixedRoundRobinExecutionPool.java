package edu.washington.escience.myria.util.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.util.internal.ConcurrentIdentityWeakKeyHashMap;

import com.google.common.base.Preconditions;

/**
 * A simple implementation of the {@link ThreadAffinityExecutorService}. It is fixed-sized. It uses a simple round-robin
 * policy to assign new tasks to an executor.
 * */
public class ThreadAffinityFixedRoundRobinExecutionPool extends AbstractExecutorService
    implements ThreadAffinityExecutorService {

  /**
   * If the pool is shutdown.
   * */
  private volatile boolean shutdown = false;

  /**
   * The executors.
   * */
  private final DefaultThreadAffinityExecutor[] executors;

  /**
   * The next executor to assign for new tasks.
   * */
  private final AtomicInteger executorIndex = new AtomicInteger(0);

  /**
   * @param poolSize thread pool size
   * */
  public ThreadAffinityFixedRoundRobinExecutionPool(final int poolSize) {
    this(poolSize, null);
  }

  /**
   * @param poolSize thread pool size
   * @param threadFactory thread factory
   * */
  public ThreadAffinityFixedRoundRobinExecutionPool(
      final int poolSize, final ThreadFactory threadFactory) {
    Preconditions.checkArgument(poolSize > 0);
    ThreadFactory tf = threadFactory;
    if (tf == null) {
      tf = new DefaultThreadFactory();
    }
    executors = new DefaultThreadAffinityExecutor[poolSize];
    for (int i = 0; i < executors.length; i++) {
      executors[i] = new DefaultThreadAffinityExecutor(tf);
    }
  }

  /**
   * {@link Runnable} or {@link Callable} -> executor.
   * */
  private final ConcurrentMap<Object, ThreadAffinityExecutor> childExecutors =
      new ConcurrentIdentityWeakKeyHashMap<Object, ThreadAffinityExecutor>();

  @Override
  public void shutdown() {
    for (DefaultThreadAffinityExecutor executor : executors) {
      executor.shutdown();
    }
    shutdown = true;
  }

  @Override
  public List<Runnable> shutdownNow() {
    ArrayList<Runnable> r = new ArrayList<Runnable>();
    for (DefaultThreadAffinityExecutor executor : executors) {
      r.addAll(executor.shutdownNow());
    }
    return r;
  }

  @Override
  public boolean isShutdown() {
    return shutdown;
  }

  @Override
  public boolean isTerminated() {
    for (DefaultThreadAffinityExecutor executor : executors) {
      if (!executor.isTerminated()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean awaitTermination(final long timeout, final TimeUnit unit)
      throws InterruptedException {
    long timeoutNanoLeft = unit.toNanos(timeout);
    long startNano = System.nanoTime();
    for (DefaultThreadAffinityExecutor executor : executors) {
      if (timeoutNanoLeft <= 0) {
        return false;
      }
      if (executor.isTerminated()) {
        continue;
      }
      executor.awaitTermination(timeoutNanoLeft, TimeUnit.NANOSECONDS);
      timeoutNanoLeft -= (System.nanoTime() - startNano);
    }
    return true;
  }

  /**
   * @param task the Runnable or Callable task
   * @return the executor for the task.
   * */
  private ThreadAffinityExecutor getExecutor0(final Object task) {
    Preconditions.checkNotNull(task);
    ThreadAffinityExecutor executor = childExecutors.get(task);
    if (executor == null) {
      executor = nextExecutor();
      ThreadAffinityExecutor old = childExecutors.putIfAbsent(task, executor);
      if (old != null) {
        executor = old;
      }
    }
    return executor;
  }

  @Override
  public void execute(final Runnable command) {
    this.submit(command);
  }

  @Override
  public ExecutionFuture<?> submit(final Runnable task) {
    return getExecutor(task).submit(Executors.callable(task));
  }

  @Override
  public <T> ExecutionFuture<T> submit(final Runnable task, final T result) {
    return getExecutor(task)
        .submit(
            new Callable<T>() {
              @Override
              public T call() throws Exception {
                task.run();
                return result;
              }
            });
  }

  @Override
  public <T> ExecutionFuture<T> submit(final Callable<T> task) {
    return getExecutor(task).submit(task);
  }

  /**
   * @return a {@link ThreadAffinityExecutor}. The jobs submitted through
   * */
  @Override
  public final ThreadAffinityExecutor nextExecutor() {
    int i = 0;
    int newI = 1;
    while (true) {
      i = executorIndex.get();
      newI = (i + 1) % executors.length;
      if (executorIndex.compareAndSet(i, newI)) {
        break;
      }
    }
    return executors[i];
  }

  @Override
  public ThreadAffinityExecutor getExecutor(final Runnable task) {
    return getExecutor0(task);
  }

  @Override
  public ThreadAffinityExecutor getExecutor(final Callable<?> task) {
    return getExecutor0(task);
  }
}
