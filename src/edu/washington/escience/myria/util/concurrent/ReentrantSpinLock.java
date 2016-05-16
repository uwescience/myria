package edu.washington.escience.myria.util.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * A simple implementation of a reentrant spin lock. A spin lock may have better performance than blocking locks if the
 * lock holding code block is very small.
 * */
public class ReentrantSpinLock implements Lock {

  /**
   * Record the thread which currently holds the lock.
   * */
  private final AtomicReference<Thread> spinLock = new AtomicReference<Thread>();

  /**
   * Number of hold count.
   * */
  private int numHoldCount = 0;

  /**
   * Do the spin lock.
   * */
  @Override
  public final void lock() {
    Thread currentT = Thread.currentThread();
    if (spinLock.get() != currentT) {
      while (!spinLock.compareAndSet(null, currentT)) {
        if (currentT.isInterrupted()) {
          return;
        }
      }
    }
    numHoldCount++;
  }

  /**
   * Unlock this lock.
   * */
  @Override
  public final void unlock() {
    Thread currentThread = Thread.currentThread();
    if (spinLock.get() != currentThread) {
      throw new IllegalStateException(
          "The current thread " + currentThread + " does not hold the lock.");
    }
    numHoldCount--;
    if (numHoldCount <= 0) {
      spinLock.set(null);
    }
  }

  @Override
  public void lockInterruptibly() throws InterruptedException {
    Thread currentT = Thread.currentThread();
    while (!this.tryLock()) {
      if (currentT.isInterrupted()) {
        throw new InterruptedException();
      }
    }
  }

  @Override
  public boolean tryLock() {
    Thread currentT = Thread.currentThread();
    if (spinLock.get() != currentT) {
      if (spinLock.compareAndSet(null, currentT)) {
        numHoldCount++;
        return true;
      }
    } else {
      numHoldCount++;
      return true;
    }
    return false;
  }

  @Override
  public boolean tryLock(final long time, final TimeUnit unit) throws InterruptedException {
    long start = System.nanoTime();
    long waitMaxNano = unit.toNanos(time);
    Thread currentT = Thread.currentThread();

    while (System.nanoTime() - start < waitMaxNano && !this.tryLock()) {
      if (currentT.isInterrupted()) {
        throw new InterruptedException();
      }
    }

    return isHoldingLock();
  }

  @Override
  public Condition newCondition() {
    // TODO Auto-generated method stub
    return null;
  }

  /**
   * @return check if current thread is holding the lock.
   * */
  public boolean isHoldingLock() {
    return spinLock.get() == Thread.currentThread();
  }
}
