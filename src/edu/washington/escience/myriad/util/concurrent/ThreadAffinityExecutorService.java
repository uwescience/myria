package edu.washington.escience.myriad.util.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class provides the following service.
 * <ol>
 * <li>The same {@link Runnable} or {@link Callable} instance submitted through this class will always get executed in
 * the same {@link Thread}</li>
 * <li>A {@link ThreadAffinityExecutor} can be got by calling {@link ThreadAffinityExecutorService#nextExecutor()}. All
 * the tasks submitted through the return executor will be executed in the same thread</li>
 * </ol>
 * */
public interface ThreadAffinityExecutorService extends ExecutorService {

  /**
   * A simple thread factory for use as the default choice if no {@link ThreadFactory} is given in the implementation
   * classes.
   * */
  class DefaultThreadFactory implements ThreadFactory {

    /**
     * id generator.
     * */
    private static final AtomicLong ID_GENERATOR = new AtomicLong(0);

    @Override
    public Thread newThread(final Runnable r) {
      return new Thread(ThreadAffinityExecutorService.class.getSimpleName() + "#" + ID_GENERATOR.getAndIncrement()) {
        @Override
        public void run() {
          r.run();
        }
      };
    }
  }

  /**
   * @param task the {@link Runnable}
   * @return the executor for the task.
   * */
  ThreadAffinityExecutor getExecutor(final Runnable task);

  /**
   * @param task the {@link Callable} task
   * @return the executor for the task.
   * */
  ThreadAffinityExecutor getExecutor(final Callable<?> task);

  /**
   * @return a {@link ThreadAffinityExecutor}.
   * */
  ThreadAffinityExecutor nextExecutor();

}
