package edu.washington.escience.myria.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;

import com.google.common.base.Preconditions;
import edu.washington.escience.myria.io.AmazonS3Source;
import edu.washington.escience.myria.io.UriSource;

/**
 * Generic utilities for Myria.
 */
public final class MyriaUtils {
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(MyriaUtils.class);
  /**
   * Utility classes should not be instantiated.
   */
  private MyriaUtils() {}

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
        Preconditions.checkArgument(
            oldV.equals(newV),
            "New value %s for key %s does not match old value %s",
            newV,
            newK,
            oldV);
      }
    }
  }

  /**
   * Ensure that the given object is a valid Myria object type and can be stored in e.g., a Column or a Field.
   *
   * @param o the object to be tested.
   * @return o.
   * @throws IllegalArgumentException if the object is not a valid Myria type.
   */
  public static Object ensureObjectIsValidType(final Object o) throws IllegalArgumentException {
    if (o instanceof Boolean) {
      return o;
    }
    if (o instanceof Double || o instanceof Float) {
      return o;
    }
    if (o instanceof Integer || o instanceof Long) {
      return o;
    }
    if (o instanceof DateTime) {
      return o;
    }
    if (o instanceof String) {
      return o;
    }
    if (o instanceof ByteBuffer) {
      return o;
    }
    if (o == null) {
      // allow initialization with a null object -- needed for BlobType.
      return o;
    }
    throw new IllegalArgumentException(
        "Object of type " + o.getClass() + " is not a valid Myria type");
  }
  /**
   * This function is called by DownloadBlob expression. It cannot throw an exception as it
   * is a complied expression, so, it returns null if there is an error in retriving the binary data.
   * @param filename uri of the binary data.
   * @return Binary data in ByteBuffer format.
   */
  public static ByteBuffer getBlob(final String filename) {
    try {
      UriSource file = new UriSource(filename);
      InputStream is = file.getInputStream();
      return ByteBuffer.wrap(IOUtils.toByteArray(is));

    } catch (Exception e) {
      LOGGER.debug(e.getMessage());
      return null;
    }
  }

  /**
   * casts blob {@code v} to an {@code int}, using big-endian byte order
   * @param v the blob to cast
   * @return the value of {@code v} as an {@code int}
   * @throws IndexOutOfBoundsException if {@code v} is smaller or larger than an {@code int}
   */
  public static int castBlobToInt(ByteBuffer v) throws IndexOutOfBoundsException {
    v.rewind();
    if (v.limit() == Integer.BYTES) {
      return v.getInt();
    } else {
      throw new IndexOutOfBoundsException(
          "Blob has " + v.limit() + " bytes, expected " + Integer.BYTES);
    }
  }

  /**
   * casts blob {@code v} to a {@code long}, using big-endian byte order
   * @param v the blob to cast
   * @return the value of {@code v} as a {@code long}
   * @throws IndexOutOfBoundsException if {@code v} is smaller or larger than a {@code long}
   */
  public static long castBlobToLong(ByteBuffer v) throws IndexOutOfBoundsException {
    v.rewind();
    if (v.limit() == Long.BYTES) {
      return v.getLong();
    } else {
      throw new IndexOutOfBoundsException(
          "Blob has " + v.limit() + " bytes, expected " + Long.BYTES);
    }
  }

  /**
   * This function replaces the `s3` scheme with the `s3a` scheme,
   * to force use of the new S3A Hadoop filesystem.
   * @param uri input URI
   * @return normalized URI
   * @throws URISyntaxException
   */
  public static URI normalizeS3Uri(URI uri) throws URISyntaxException {
    if (uri.getScheme() != null && uri.getScheme().equals("s3")) {
      uri =
          new URI(
              "s3a",
              uri.getUserInfo(),
              uri.getHost(),
              uri.getPort(),
              uri.getPath(),
              uri.getQuery(),
              uri.getFragment());
    }
    return uri;
  }
}
