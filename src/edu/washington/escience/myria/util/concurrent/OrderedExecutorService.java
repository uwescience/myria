/*
 * This class is modified from Netty's org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor.
 *
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package edu.washington.escience.myria.util.concurrent;

import java.util.IdentityHashMap;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jboss.netty.util.internal.ConcurrentIdentityWeakKeyHashMap;

/**
 * A {@link ThreadPoolExecutor} which makes sure the events from the same {@link id} are executed sequentially.
 * <p>
 *
 * <h3>Event execution order</h3>
 *
 * For example, let's say there are two executor threads that handle the events from the two channels:
 *
 * <pre>
 *           -------------------------------------&gt; Timeline ------------------------------------&gt;
 *
 * Thread X: --- id A (Event A1) --.   .-- id B (Event B2) --- id B (Event B3) ---&gt;
 *                                      \ /
 *                                       X
 *                                      / \
 * Thread Y: --- id B (Event B1) --'   '-- id A (Event A2) --- id A (Event A3) ---&gt;
 * </pre>
 * As you see, the events from different channels are independent from each other. That is, an event of id B will not be
 * blocked by an event of id A and vice versa, unless the thread pool is exhausted.
 * <p>
 * Also, it is guaranteed that the invocation will be made sequentially for the events from the same channel. For
 * example, the event A2 is never executed before the event A1 is finished.
 * <p>
 * However, it is not guaranteed that the invocation will be made by the same thread for the same channel. The events
 * from the same channel can be executed by different threads. For example, the Event A2 is executed by the thread Y
 * while the event A1 was executed by the thread X.
 *
 * <h3>Using a different key other than {@link id} to maintain event order</h3>
 * <p>
 * {@link OrderedExecutorService} uses a {@link id} as a key that is used for maintaining the event execution order, as
 * explained in the previous section. Alternatively, you can extend it to change its behavior. For example, you can
 * change the key to the remote IP of the peer:
 *
 * <pre>
 * public class RemoteAddressBasedOMATPE extends {@link OrderedExecutorService} {
 *
 *     ... Constructors ...
 *
 *     {@code @Override}
 *     protected ConcurrentMap&lt;Object, Executor&gt; newChildExecutorMap() {
 *         // The default implementation returns a special ConcurrentMap that
 *         // uses identity comparison only (see {@link IdentityHashMap}).
 *         // Because SocketAddress does not work with identity comparison,
 *         // we need to employ more generic implementation.
 *         return new ConcurrentHashMap&lt;Object, Executor&gt;
 *     }
 *
 *     // Make public so that you can call from anywhere.
 *     public boolean removeChildExecutor(Object key) {
 *         super.removeChildExecutor(key);
 *     }
 * }
 * </pre>
 *
 * Please be very careful of memory leak of the child executor map. You must call {@link #removeChildExecutor(Object)}
 * when the life cycle of the key ends (e.g. all connections from the same IP were closed.) Also, please keep in mind
 * that the key can appear again after calling {@link #removeChildExecutor(Object)} (e.g. a new connection could come in
 * from the same old IP after removal.) If in doubt, prune the old unused or stall keys from the child executor map
 * periodically:
 *
 * @param <KEY> the type of the key.
 */
public class OrderedExecutorService<KEY> extends ThreadPoolExecutor {

  /**
   * Default keep alive time interval: 10 minutes.
   * */
  public static final int DEFAULT_KEEP_ALIVE = 60 * 10;

  /**
   * The Runnable interface. Only the instances with this interface will be ordered when execute.
   *
   * @param <KEY> the type of the key
   * */
  public interface KeyRunnable<KEY> extends Runnable {
    /**
     * @return the key of this Runnable.
     * */
    KEY getKey();
  }

  /**
   * Key -> pseudo executor.
   * */
  private final ConcurrentMap<Object, Executor> childExecutors =
      new ConcurrentIdentityWeakKeyHashMap<Object, Executor>();

  /**
   * Creates a new instance.
   *
   * @param corePoolSize the minimum number of active threads
   */
  public OrderedExecutorService(final int corePoolSize) {
    this(corePoolSize, corePoolSize);
  }

  /**
   * Creates a new instance.
   *
   * @param corePoolSize the minimum number of active threads
   * @param maximumPoolSize the maximum number of active threads
   */
  public OrderedExecutorService(final int corePoolSize, final int maximumPoolSize) {
    this(corePoolSize, maximumPoolSize, DEFAULT_KEEP_ALIVE, TimeUnit.SECONDS);
  }

  /**
   * Creates a new instance.
   *
   * @param corePoolSize the maximum number of active threads
   * @param maximumPoolSize the maximum number of active threads
   * @param keepAliveTime the amount of time for an inactive thread to shut itself down
   * @param unit the {@link TimeUnit} of {@code keepAliveTime}
   */
  public OrderedExecutorService(
      final int corePoolSize,
      final int maximumPoolSize,
      final long keepAliveTime,
      final TimeUnit unit) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, new LinkedBlockingQueue<Runnable>());
  }

  /**
   * Creates a new instance.
   *
   * @param corePoolSize the maximum number of active threads
   * @param maximumPoolSize the maximum number of active threads
   * @param keepAliveTime the amount of time for an inactive thread to shut itself down
   * @param unit the {@link TimeUnit} of {@code keepAliveTime}
   * @param threadFactory the {@link ThreadFactory} of this pool
   */
  public OrderedExecutorService(
      final int corePoolSize,
      final int maximumPoolSize,
      final long keepAliveTime,
      final TimeUnit unit,
      final ThreadFactory threadFactory) {
    super(
        corePoolSize,
        maximumPoolSize,
        keepAliveTime,
        unit,
        new LinkedBlockingQueue<Runnable>(),
        threadFactory);
  }

  /**
   * Creates a new instance.
   *
   * @param corePoolSize the maximum number of active threads
   * @param maximumPoolSize the maximum number of active threads
   * @param threadFactory the {@link ThreadFactory} of this pool
   */
  public OrderedExecutorService(
      final int corePoolSize, final int maximumPoolSize, final ThreadFactory threadFactory) {
    super(
        corePoolSize,
        maximumPoolSize,
        DEFAULT_KEEP_ALIVE,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue<Runnable>(),
        threadFactory);
  }

  /**
   * @return key -> pseudo executor map.
   * */
  protected ConcurrentMap<Object, Executor> newChildExecutorMap() {
    return new ConcurrentIdentityWeakKeyHashMap<Object, Executor>();
  }

  @Override
  public final Future<?> submit(final Runnable task) {
    throw new UnsupportedOperationException("Not yet implemented. Use execute(Runnable) instead.");
  }

  @Override
  public final <T> Future<T> submit(final Callable<T> task) {
    throw new UnsupportedOperationException("Not yet implemented. Use execute(Runnable) instead.");
  }

  @Override
  public final <T> Future<T> submit(final Runnable task, final T result) {
    throw new UnsupportedOperationException("Not yet implemented. Use execute(Runnable) instead.");
  }

  /**
   * Executes the specified task concurrently while maintaining the event order.
   *
   * @param task the task.
   */
  @Override
  public final void execute(final Runnable task) {
    if (!(task instanceof KeyRunnable)) {
      super.execute(task);
    } else {
      @SuppressWarnings("unchecked")
      KeyRunnable<KEY> kr = (KeyRunnable<KEY>) task;
      getChildExecutor(kr.getKey()).execute(task);
    }
  }

  /**
   * @return the pseudo executor for a key.
   * @param key the key.
   * */
  private Executor getChildExecutor(final KEY key) {
    Executor executor = childExecutors.get(key);
    if (executor == null) {
      executor = new ChildExecutor();
      Executor oldExecutor = childExecutors.putIfAbsent(key, executor);
      if (oldExecutor != null) {
        executor = oldExecutor;
      }
    }

    return executor;
  }

  //
  // private void onAfterExecute(final Runnable r, final Throwable t) {
  // afterExecute(r, t);
  // }

  /**
   * Pseudo executor for a key.
   * */
  private final class ChildExecutor implements Executor, Runnable {
    /**
     * The Runnable tasks under the common key.
     * */
    private final Queue<Runnable> tasks = new ConcurrentLinkedQueue<Runnable>();
    /**
     * If any Runnable task in this task group is running.
     * */
    private final AtomicBoolean isRunning = new AtomicBoolean();

    @Override
    public void execute(final Runnable command) {
      // TODO: What todo if the add return false ?
      if (!tasks.add(command)) {
        throw new OutOfMemoryError(
            "Too many runnable tasks under a key. Only " + Integer.MAX_VALUE + " can be held.");
      }

      if (!isRunning.get()) {
        OrderedExecutorService.this.execute(this);
      }
    }

    @Override
    public void run() {
      boolean acquired;

      // check if its already running by using CAS. If so just return here. So in the worst case the thread
      // is executed and do nothing
      if (isRunning.compareAndSet(false, true)) {
        acquired = true;
        try {
          Thread thread = Thread.currentThread();
          for (; ; ) {
            final Runnable task = tasks.poll();
            // if the task is null we should exit the loop
            if (task == null) {
              break;
            }

            boolean ran = false;
            beforeExecute(thread, task);
            try {
              task.run();
              ran = true;
              afterExecute(task, null);
            } catch (RuntimeException e) {
              if (!ran) {
                afterExecute(task, e);
              }
              throw e;
            }
          }
        } finally {
          // set it back to not running
          isRunning.set(false);
        }

        if (acquired && !isRunning.get() && tasks.peek() != null) {
          OrderedExecutorService.this.execute(this);
        }
      }
    }
  }
}
