package edu.washington.escience.myriad.util;

import java.util.concurrent.atomic.AtomicReference;

/**
 * A simple implementation of a reentrant spin lock. A spin lock may have better performance than blocking locks if the
 * lock holding code block is very small.
 * */
public class ReentrantSpinLock {

  /**
   * Record the thread which currently holds the lock.
   * */
  private final AtomicReference<Thread> spinLock = new AtomicReference<Thread>();

  /**
   * Do the spin lock.
   * */
  public final void lock() {
    Thread currentT = Thread.currentThread();
    if (spinLock.get() != currentT) {
      while (!spinLock.compareAndSet(null, currentT)) {
        ;
      }
    }
  }

  /**
   * Unlock this lock.
   * */
  public final void unlock() {
    spinLock.compareAndSet(Thread.currentThread(), null);
  }

}
