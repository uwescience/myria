package edu.washington.escience.myriad.util;

import java.util.Collection;

/**
 * Generic utilities for Myria.
 * 
 * @author dhalperi
 */
public final class MyriaUtils {
  /**
   * Utility classes should not be instantiated.
   */
  private MyriaUtils() {
  }

  /**
   * Convert a collection of integers to an int[].
   * 
   * @param input the collection of integers.
   * @return an int[] containing the given integers.
   */
  public static int[] integerCollectionToIntArray(final Collection<Integer> input) {
    int[] output = new int[input.size()];
    int i = 0;
    for (int value : input) {
      output[i] = value;
      ++i;
    }
    return output;
  }
}