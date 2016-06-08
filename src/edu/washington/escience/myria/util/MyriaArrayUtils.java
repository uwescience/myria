package edu.washington.escience.myria.util;

import java.util.Arrays;
import java.util.Set;

import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;

/**
 * Array related utility functions.
 *
 */
public final class MyriaArrayUtils extends org.apache.commons.lang3.ArrayUtils {

  /** The logger for this class. */
  protected static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(MyriaArrayUtils.class);

  /**
   * Utility classes should not be instantiated.
   */
  private MyriaArrayUtils() {}

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
   * Flatten a 2D array into a 1D array.
   *
   * @param arr input 2D array
   * @return the flattened 1D array ()
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
   * Flatten a 2D array into a 1D array then sort it.
   *
   * @param arr input 2D array
   * @return the flattened and sorted array
   */
  public static int[] arrayFlattenThenSort(final int[][] arr) {
    int[] result = arrayFlatten(arr);
    Arrays.sort(result);
    return result;
  }

  /**
   * @param length size of 2d index
   * @return a 2D index like { {0},{1},{2},..., {n} }
   */
  public static int[][] create2DVerticalIndex(final int length) {
    int[][] result = new int[length][];
    for (int i = 0; i < length; i++) {
      result[i] = new int[] {i};
    }
    return result;
  }

  /**
   * @param length size of 2d index
   * @return a 2D index like { {0,1,2,...,n} }
   */
  public static int[][] create2DHorizontalIndex(final int length) {
    int[][] result = new int[1][length];
    for (int i = 0; i < length; i++) {
      result[0][i] = i;
    }
    return result;
  }

  /**
   * convert a 1D array into a 2D array.
   *
   * @param arr input 1D array.
   * @return 2D array returned.
   * */
  public static int[][] get2DArray(final int[] arr) {
    int[][] result = new int[arr.length][];
    for (int i = 0; i < arr.length; i++) {
      result[i] = new int[] {arr[i]};
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
      throw new IllegalArgumentException(
          "The array " + Arrays.toString(maybeSetArray) + " is not a set");
    }
    return r;
  }

  /**
   * Check if an array of int is a set. If not, log a warning.
   *
   * @param maybeSetArray data array
   * @return the array itself
   * */
  public static int[] warnIfNotSet(final int[] maybeSetArray) {
    try {
      return checkSet(maybeSetArray);
    } catch (IllegalArgumentException e) {
      LOGGER.warn("Array " + Arrays.toString(maybeSetArray) + " is not a set", e);
      return maybeSetArray;
    }
  }

  /**
   * Check if an array of int is a set.
   *
   * @param maybeSetArray data array
   * @return the array it self
   * @throws IllegalArgumentException if the data array is not a set.
   * */
  public static int[] checkSet(final int[] maybeSetArray) {
    Set<Integer> tmp = Sets.newHashSet(Ints.asList(maybeSetArray));
    if (maybeSetArray.length != tmp.size()) {
      throw new IllegalArgumentException(
          "The array " + Arrays.toString(maybeSetArray) + " is not a set");
    }
    return maybeSetArray;
  }

  /**
   * Check if an array of int is a list of valid indices of another data array .
   *
   * @param arrayOfIndices indices array
   * @param size the size of another array, i.e. the data array
   * @return the arrayOfIndices it self
   * @throws IllegalArgumentException if check fails.
   * */
  public static int[] checkPositionIndices(final int[] arrayOfIndices, final int size) {
    for (int i : arrayOfIndices) {
      Preconditions.checkPositionIndex(i, size);
    }
    return arrayOfIndices;
  }
}
