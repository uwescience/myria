/**
 *
 */
package edu.washington.edu.escience.myria.perfenforce;

import java.util.HashSet;
import java.util.Set;

/**
 * Helper Methods
 */
public class PerfEnforceUtils {
  public static Set<Integer> getRangeSet(final int limit) {
    Set<Integer> seq = new HashSet<Integer>();
    for (int i = 1; i <= limit; i++) {
      seq.add(i);
    }
    return seq;
  }

  public static int[] getRangeInclusive(final int min, final int max) {
    int numberElements = (max - min) + 1;
    int[] intArray = new int[numberElements];
    for (int i = 0; i < numberElements; i++) {
      intArray[i] = min + i;
    }
    return intArray;
  }
}
