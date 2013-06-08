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

  /**
   * Helper function that generates an array of the numbers 0..max-1.
   * 
   * @param max the size of the array.
   * @return an array of the numbers 0..max-1.
   */
  public static int[] range(final int max) {
    int[] ret = new int[max];
    for (int i = 0; i < max; ++i) {
      ret[i] = i;
    }
    return ret;
  }

}