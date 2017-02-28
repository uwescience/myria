package edu.washington.escience.myria.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;

/**
 * Array related utility functions.
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
   */
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
   */
  public static int[] arrayFillAndReturn(final int[] arr, final int e) {
    Arrays.fill(arr, e);
    return arr;
  }

  /**
   * @param n size of 2d index
   * @return a 2D index like { {0},{1},{2},..., {n-1} }
   */
  public static List<List<Integer>> create2DVerticalIndexList(final int n) {
    List<List<Integer>> result = new ArrayList<List<Integer>>();
    for (int i = 0; i < n; i++) {
      result.add(Lists.newArrayList(i));
    }
    return result;
  }

  /**
   * @param n size of 2d index
   * @return a 2D index like { {0,1,2,...,n-1} }
   */
  public static List<List<Integer>> create2DHorizontalIndexList(final int n) {
    List<List<Integer>> result = new ArrayList<List<Integer>>();
    result.add(new ArrayList<Integer>());
    for (int i = 0; i < n; i++) {
      result.get(0).add(i);
    }
    return result;
  }

  /**
   * convert a 1D array into a 2D array.
   *
   * @param arr input 1D array.
   * @return 2D array returned.
   */
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
   */
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
   */
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
   */
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
   */
  public static int[] checkPositionIndices(final int[] arrayOfIndices, final int size) {
    for (int i : arrayOfIndices) {
      Preconditions.checkPositionIndex(i, size);
    }
    return arrayOfIndices;
  }

  /**
   * Helper function that generates an array of the numbers in [start, start+length).
   *
   * @param start the size of the array.
   * @param length the length of the array.
   * @return an array of the numbers [start, start+length).
   */
  public static int[] range(final int start, final int length) {
    return IntStream.range(start, start + length).toArray();
  }
}
