package edu.washington.escience.myria.util.concurrent;

import java.util.concurrent.locks.ReentrantLock;

/**
 * A simple wrapper of a {@link ReentrantLock}. It can be used within Java>7 's auto resource close framework.
 */
public class ClosableReentrantLock extends ReentrantLock implements AutoCloseable {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  /**
   * Call this method in try block to gain the lock.
   *
   * @return this lock.
   */
  public ClosableReentrantLock open() {
    lock();
    return this;
  }

  @Override
  public void close() {
    unlock();
  }
}
