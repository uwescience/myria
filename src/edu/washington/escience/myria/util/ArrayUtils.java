package edu.washington.escience.myria.util;

import java.util.Arrays;

import com.google.common.collect.ImmutableSet;

/**
 * Array related utility functions.
 * 
 */
public final class ArrayUtils extends org.apache.commons.lang3.ArrayUtils {

  /**
   * Utility classes should not be instantiated.
   * 
   * Not a bug. Bugfix has a false alert. Just ignore it.
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
   * Flatten a 2D arrary into a 1D arrary.
   * 
   * @param arr input 2D array
   * @return the flattened 1D arrary ()
   */
  public static int[] arrayFlatten(final int[][] arr) {
    int size = 0;
    for (int[] e : arr) {
      size += e.length;
    }

    int[] result = new int[size];
    int i = 0;
    for (int[] e : arr) {
      for (int v : e) {
        result[i] = v;
        i++;
      }
    }

    return result;
  }

  /**
   * convert a 1D array into a 2D arrary.
   * 
   * @param arr input 1D array.
   * @return 2D array returned.
   * */
  public static int[][] get2DArray(final int[] arr) {
    int[][] result = new int[arr.length][];
    for (int i = 0; i < arr.length; i++) {
      result[i] = new int[] { arr[i] };
    }
    return result;
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
