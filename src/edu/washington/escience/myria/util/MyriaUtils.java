package edu.washington.escience.myria.util;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.base.Preconditions;

/**
 * Generic utilities for Myria.
 * 
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
   * Convert a collection of integers to a sorted int[].
   * 
   * @param input the collection of integers.
   * @return an int[] containing the given integers.
   */
  public static int[] integerCollectionToIntArray(final Collection<Integer> input) {
    int[] output = new int[input.size()];
    int i = 0;
    for (int value : new TreeSet<>(input)) {
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
   * Throws an {@link IllegalArgumentException} if the specified iterable contains a null value.
   * 
   * @param <T> any object type that extends Iterable
   * @param iter the iterable
   * @param message a message to be included with the exception
   * @return {@link IllegalArgumentException} if the iterable contains a null element.
   */
  public static <T extends Iterable<?>> T checkHasNoNulls(final T iter, final String message) {
    Objects.requireNonNull(iter, message);
    int i = 0;
    for (Object o : iter) {
      Preconditions.checkNotNull(o, "%s [element %s]", message, i);
      ++i;
    }
    return iter;
  }

  /**
   * Copy all mappings from the source to the destination, ensuring that if any keys were already present, then the
   * values match. This is sort of a "map Union" operator.
   * 
   * @param <K> the type of the keys.
   * @param <V> the type of the values.
   * @param source the new mappings to be added.
   * @param dest the destination for new mappings, which may already has some mappings.
   */
  public static <K, V> void putNewVerifyOld(final Map<K, V> source, final Map<K, V> dest) {
    for (Map.Entry<K, V> entry : source.entrySet()) {
      K newK = entry.getKey();
      V newV = entry.getValue();
      V oldV = dest.get(newK);
      if (oldV == null) {
        dest.put(newK, newV);
      } else {
        Preconditions.checkArgument(oldV.equals(newV), "New value %s for key %s does not match old value %s", newV,
            newK, oldV);
      }
    }
  }
}
