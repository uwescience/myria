package edu.washington.escience.myriad.util;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Util methods for atomic variables, mainly for bitwise operations.
 * */
public final class AtomicUtils {

  /**
   * Make the util class uninstantiable.
   * */
  private AtomicUtils() {
  }

  /**
   * Atomically do bitwise OR between the value in the AtomicInteger and the toOrValue, and set the new value to the
   * AtomicInteger and also return the new value.
   * 
   * @param ai the atomic variable to operate on
   * @param toOrValue the value to do the OR operation
   * @return the new value.
   * */
  public static int bitwiseOrAndGet(final AtomicInteger ai, final int toOrValue) {
    int oldValue = ai.get();
    int newValue = oldValue | toOrValue;
    while (oldValue != newValue && !ai.compareAndSet(oldValue, newValue)) {
      oldValue = ai.get();
      newValue = oldValue | toOrValue;
    }
    return newValue;
  }

  /**
   * Atomically do bitwise AND between the value in the AtomicInteger and the toAndValue, and set the new value to the
   * AtomicInteger and also return the new value.
   * 
   * @param ai the atomic variable to operate on
   * @param toAndValue the value to do the OR operation
   * @return the new value.
   * */
  public static int bitwiseAndAndGet(final AtomicInteger ai, final int toAndValue) {
    int oldValue = ai.get();
    int newValue = oldValue & toAndValue;
    while (oldValue != newValue && !ai.compareAndSet(oldValue, newValue)) {
      oldValue = ai.get();
      newValue = oldValue & toAndValue;
    }
    return newValue;
  }

}
