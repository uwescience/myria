package edu.washington.escience.myriad.util;

import java.util.Arrays;

import com.google.common.collect.ImmutableSet;

/**
 * Array related utility functions.
 * 
 */
public final class ArrayUtils extends org.apache.commons.lang3.ArrayUtils {

  /**
   * Utility classes should not be instantiated.
   */
  private ArrayUtils() {
  }

  /**
   * Fill object array.
   * 
   * @return the filled array.
   * @param arr the array to fill
   * @param e the element to fill.
   * */
  public static Object[] arrayFillAndReturn(final Object[] arr, final Object e) {
    Arrays.fill(arr, e);
    return arr;
  }

  /**
   * Fill int array.
   * 
   * @return the filled array.
   * @param arr the array to fill
   * @param e the element to fill.
   * */
  public static int[] arrayFillAndReturn(final int[] arr, final int e) {
    Arrays.fill(arr, e);
    return arr;
  }

  /**
   * @param maybeSetArray data array
   * @return An ImmutableSet of the data array if the data array is actually a set
   * @throws IllegalArgumentException if the data array is not a set.
   * @param <E> array element type.
   * */
  public static <E> ImmutableSet<E> checkSet(final E[] maybeSetArray) {
    ImmutableSet.Builder<E> builder = new ImmutableSet.Builder<>();
    for (E i : maybeSetArray) {
      builder.add(i);
    }
    ImmutableSet<E> r = builder.build();
    if (r.size() != maybeSetArray.length) {
      throw new IllegalArgumentException("The array is not a set");
    }
    return r;
  }

}
