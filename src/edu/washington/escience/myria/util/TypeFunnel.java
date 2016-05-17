package edu.washington.escience.myria.util;

import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

/**
 * A TypeFunnel is a Guava {@link Funnel} intended for use in hashing.
 *
 *
 */
public enum TypeFunnel implements Funnel<Object> {
  /** Enforce singleton. */
  INSTANCE;

  @Override
  public void funnel(final Object o, final PrimitiveSink into) {
    if (o instanceof Boolean) {
      into.putBoolean((Boolean) o);
    } else if (o instanceof Double) {
      into.putDouble((Double) o);
    } else if (o instanceof Float) {
      into.putFloat((Float) o);
    } else if (o instanceof Integer) {
      into.putInt((Integer) o);
    } else if (o instanceof Long) {
      into.putLong((Long) o);
    } else if (o instanceof String) {
      into.putUnencodedChars((String) o);
    }
  }
}
