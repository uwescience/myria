package edu.washington.escience.myria.util.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.util.JVMUtils;

/**
 * The {@link ExecutionFuture} implementation that is Callable and Runnable. The state of the future is set
 * automatically by the execution of this task.
 *
 * @param <T> The return type.
 */
public class ExecutableExecutionFuture<T> extends OperationFutureBase<T>
    implements ExecutionFuture<T>, Callable<T>, Runnable {

  /**
   * The {@link Callable} who caused the creation of this future.
   * */
  private final Callable<T> callable;

  /**
   * The thread who is executing the task.
   * */
  private volatile Thread executingThread;

  /**
   * The lock to make sure the thread interrupt action is correct in {@link this#cancel(boolean)}.
   * */
  private final Object interruptedCheckingLock = new Object();

  /**
   * Creates a new instance.
   *
   * @param callable the {@link Callable} who caused the creation of this future.
   *
   * @param cancellable {@code true} if and only if this future can be canceled
   */
  public ExecutableExecutionFuture(final Callable<T> callable, final boolean cancellable) {
    super(cancellable);
    Preconditions.checkNotNull(callable);
    this.callable = callable;
  }

  /**
   * Creates a new instance.
   *
   * @param runnable the {@link Runnable} who caused the creation of this future.
   *
   * @param cancellable {@code true} if and only if this future can be canceled
   */
  public ExecutableExecutionFuture(final Runnable runnable, final boolean cancellable) {
    this(Executors.callable(runnable, (T) null), cancellable);
  }

  @Override
  public final ExecutionFuture<T> sync() throws InterruptedException, DbException {
    sync0();
    return this;
  }

  @Override
  public final ExecutionFuture<T> syncUninterruptibly() throws DbException {
    super.syncUninterruptibly0();
    return this;
  }

  @Override
  public final ExecutionFuture<T> await() throws InterruptedException {
    super.await0();
    return this;
  }

  @Override
  public final ExecutionFuture<T> awaitUninterruptibly() {
    super.awaitUninterruptibly0();
    return this;
  }

  /**
   * running state.
   * */
  protected static final int RUNNING = 2;

  @Override
  public boolean cancel(final boolean mayInterruptIfRunning) {
    if (!isCancellable()) {
      return false;
    }

    if (isDone()) {
      return isCancelled();
    }

    if (!super.compareAndSetState(READY, CANCELL)) {
      // already running or already done
      if (mayInterruptIfRunning) {
        Thread t = executingThread;
        if (t != null) {
          // cancel in running
          if (t == Thread.currentThread()) {
            cancel();
          } else if (super.compareAndSetState(RUNNING, CANCELL)) {
            synchronized (interruptedCheckingLock) {
              t = executingThread;
              if (t != null) {
                t.interrupt();
              }
            }
            this.awaitUninterruptibly();
            wakeupWaitersAndNotifyListeners();
          }
        }
      }
    } else {
      wakeupWaitersAndNotifyListeners();
    }
    return isCancelled();
  }

  @Override
  protected boolean doCancel() {
    return this.cancel(true);
  }

  @Override
  public T get() throws InterruptedException, ExecutionException {
    this.await();
    this.checkFailure();
    return getResult();
  }

  /**
   * @throws ExecutionException if there's any error occurred during the execution of the {@link Runnable} or
   *           {@link Callable}
   * */
  private void checkFailure() throws ExecutionException {
    if (isDone() && !isSuccess()) {
      throw new ExecutionException(getCause());
    }
  }

  @Override
  public T get(final long timeout, final TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    this.await(timeout, unit);
    if (isDone()) {
      this.checkFailure();
      return getResult();
    }
    return null;
  }

  @Override
  public Callable<T> getCallable() {
    return callable;
  }

  @Override
  public ExecutionFuture<T> addListener(final OperationFutureListener listener) {
    super.addListener0(listener);
    return this;
  }

  @Override
  public ExecutionFuture<T> removeListener(final OperationFutureListener listener) {
    super.removeListener0(listener);
    return this;
  }

  @Override
  public T call() throws Exception {
    if (!compareAndSetState(READY, RUNNING)) {
      if (isDone()) {
        return getResult();
      } else {
        throw new RejectedExecutionException(
            "Another thread (" + this.executingThread + ") is executing this task");
      }
    }
    this.executingThread = Thread.currentThread();
    try {
      T result = null;
      try {
        if (Thread.interrupted()) {
          throw new InterruptedException();
        }
        result = this.callable.call();
        if (Thread.interrupted()) {
          throw new InterruptedException();
        }
      } catch (InterruptedException e) {
        if (isCancelled()) {
          // interrupted because the operation is cancelled.
          return null;
        } else {
          // unexpected interrupted, for example caused by shutdownNow
          throw e;
        }
      }
      if (setSuccess0(result)) {
        return result;
      } else {
        return null;
      }
    } catch (Throwable e) {
      setFailure0(e);
      if (e instanceof OutOfMemoryError) {
        JVMUtils.shutdownVM(e);
      }
      if (e instanceof Exception) {
        throw e;
      }
      return getResult();
    } finally {

      synchronized (interruptedCheckingLock) {
        this.executingThread = null;
        if (Thread.currentThread().isInterrupted()) {
          if (!isCancelled()) {
            throw new InterruptedException();
          }
        }
      }
    }
  }

  @Override
  public void run() {
    try {
      this.call();
    } catch (Exception e) {
      throw new Error(e);
    }
  }

  @Override
  public ExecutableExecutionFuture<T> addPreListener(final OperationFutureListener listener) {
    super.addPreListener0(listener);
    return this;
  }
}
