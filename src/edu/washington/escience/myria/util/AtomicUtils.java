package edu.washington.escience.myria.util;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Util methods for atomic variables, mainly for bitwise operations.
 * */
public final class AtomicUtils {

  /**
   * Make the util class uninstantiable.
   * */
  private AtomicUtils() {}

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

  /**
   * @return true if operation is conducted, false if the bit is already unset.
   * @param ai the AtomicInteger to operate on
   * @param n the nth bit from the least significant bit to compare, the least significant bit is 0
   * */
  public static boolean unsetBitIfSetByIndex(final AtomicInteger ai, final int n) {
    if (n >= Integer.SIZE) {
      throw new IllegalArgumentException("Out of int bit index boundary (31)");
    }
    int bitInt = 1 << n;
    int oldValue = ai.get();
    int newValue = oldValue & ~bitInt;
    while (newValue != oldValue && !ai.compareAndSet(oldValue, newValue)) {
      oldValue = ai.get();
      newValue = oldValue & ~bitInt;
    }
    return newValue != oldValue;
  }

  /**
   * @return true if operation is conducted, false if the bit is already set.
   * @param ai the AtomicInteger to operate on
   * @param value from the value get the index of the set bit from the least significant bit. Do the operation on that
   *          index.
   * */
  public static boolean unsetBitIfSetByValue(final AtomicInteger ai, final int value) {
    return unsetBitIfSetByIndex(ai, Integer.numberOfTrailingZeros(value));
  }

  /**
   * @return true if operation is conducted, false if the bit is already set.
   * @param ai the AtomicInteger to operate on
   * @param n the nth bit from the least significant bit to compare
   * */
  public static boolean setBitIfUnsetByIndex(final AtomicInteger ai, final int n) {
    if (n >= Integer.SIZE) {
      throw new IllegalArgumentException("Out of int bit index boundary (31)");
    }
    int bitInt = 1 << n;
    int oldValue = ai.get();
    int newValue = oldValue | bitInt;
    while (newValue != oldValue && !ai.compareAndSet(oldValue, newValue)) {
      oldValue = ai.get();
      newValue = oldValue | bitInt;
    }
    return newValue != oldValue;
  }

  /**
   * @return true if operation is conducted, false if the bit is already set.
   * @param ai the AtomicInteger to operate on
   * @param value from the value get the index of the set bit from the least significant bit. Do the operation on that
   *          index.
   * */
  public static boolean setBitIfUnsetByValue(final AtomicInteger ai, final int value) {
    return setBitIfUnsetByIndex(ai, Integer.numberOfTrailingZeros(value));
  }

  /**
   * @param ai the AtomicInteger to operate on
   * @param n the nth bit from the least significant bit to compare
   * */
  public static void setBitByIndex(final AtomicInteger ai, final int n) {
    setBitIfUnsetByIndex(ai, n);
  }

  /**
   * @param ai the AtomicInteger to operate on
   * @param value from the value get the index of the set bit from the least significant bit. Do the operation on that
   *          index.
   * */
  public static void setBitByValue(final AtomicInteger ai, final int value) {
    setBitByIndex(ai, Integer.numberOfTrailingZeros(value));
  }

  /**
   * @param ai the AtomicInteger to operate on
   * @param n the nth bit from the least significant bit to compare
   * */
  public static void unsetBitByIndex(final AtomicInteger ai, final int n) {
    unsetBitIfSetByIndex(ai, n);
  }

  /**
   * @param ai the AtomicInteger to operate on
   * @param value from the value get the index of the set bit from the least significant bit. Do the operation on that
   *          index.
   * */
  public static void unsetBitByValue(final AtomicInteger ai, final int value) {
    unsetBitByIndex(ai, Integer.numberOfTrailingZeros(value));
  }
}
