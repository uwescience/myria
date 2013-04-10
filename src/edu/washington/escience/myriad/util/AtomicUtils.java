package edu.washington.escience.myriad.util;

import java.util.concurrent.atomic.AtomicInteger;

public final class AtomicUtils {

  private AtomicUtils() {
  }

  public static int bitwiseOrAndGet(final AtomicInteger ai, final int toOrValue) {
    int oldValue = ai.get();
    int newValue = oldValue | toOrValue;
    while (oldValue != newValue && !ai.compareAndSet(oldValue, newValue)) {
      oldValue = ai.get();
      newValue = oldValue | toOrValue;
    }
    return newValue;
  }

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
