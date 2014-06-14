package edu.washington.escience.myria.util;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

import com.google.common.base.Preconditions;

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
   * Get the only element in single-element list.
   * 
   * @param input a non-null list of a single object.
   * @param <T> the type of the objects in the list.
   * @return the object.
   */
  public static <T> T getSingleElement(final List<T> input) {
    Objects.requireNonNull(input);
    Preconditions.checkArgument(input.size() == 1, "list must contain a single element");
    return input.get(0);
  }

  /**
   * Get the only element in single-element set.
   * 
   * @param input a non-null set of a single object.
   * @param <T> the type of the objects in the set.
   * @return the object.
   */
  public static <T> T getSingleElement(final Set<T> input) {
    Objects.requireNonNull(input);
    Preconditions.checkArgument(input.size() == 1, "list must contain a single element");
    for (T e : input) {
      /* return only one time with the first element */
      return e;
    }
    return null;
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

  /**
   * Throws an {@link IllegalArgumentException} if the specified collection contains a null value.
   * 
   * @param iter the iterable
   * @param message a message to be included with the exception
   * @return {@link IllegalArgumentException} if the iterable contains a null element.
   */
  public static Iterable<?> checkHasNoNulls(final Iterable<?> iter, final String message) {
    for (Object o : iter) {
      Preconditions.checkNotNull(o, message);
    }
    return iter;
  }

  /**
   * Generates a random sample from a set.
   * 
   * @param <T> the set type
   * 
   * @param items the superset
   * @param m the subset size
   * @return the random subset
   */
  public static <T> Set<T> randomSample(final List<T> items, final int m) {
    Random rnd = new Random();
    HashSet<T> res = new HashSet<T>(m);
    int n = items.size();
    for (int i = n - m; i < n; i++) {
      int pos = rnd.nextInt(i + 1);
      T item = items.get(pos);
      if (res.contains(item)) {
        res.add(items.get(i));
      } else {
        res.add(item);
      }
    }
    return res;
  }

  /**
   * Convert a double collection of integers to an int[][].
   * 
   * @param input the collection of integers.
   * @return an int[][] containing the given integers.
   * @throws Exception in case of different inner collection sizes
   */
  public static int[][] integerDoubleCollectionToIntArray(final Collection<Set<Integer>> input) throws Exception {
    Preconditions.checkArgument(input.size() > 0);

    Collection<Integer> inner0 = (Collection<Integer>) input.toArray()[0];
    int[][] output = new int[input.size()][inner0.size()];
    int i = 0;
    for (Collection<Integer> inner : input) {
      if (inner.size() != inner0.size()) {
        throw new Exception("All inner collections must have same size to be converted into a matrix.");
      }
      int j = 0;
      for (int value : inner) {
        output[i][j] = value;
        ++j;
      }
      ++i;
    }
    return output;
  }

}