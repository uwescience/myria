package edu.washington.escience.myria.storage;

import javax.annotation.Nonnull;

import org.joda.time.DateTime;

/**
 * An interface for a readable object holding a single column of tuples.
 */
public interface ReadableColumn extends ColumnInterface {
  /**
   * Returns the boolean value at the specified row in this column.
   *
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   * @throws UnsupportedOperationException if this column does not support this type.
   */
  boolean getBoolean(final int row);

  /**
   * Returns the {@link DateTime} value at the specified row in this column.
   *
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   * @throws UnsupportedOperationException if this column does not support this type.
   */
  @Nonnull
  DateTime getDateTime(final int row);

  /**
   * Returns the double value at the specified row in this column.
   *
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   * @throws UnsupportedOperationException if this column does not support this type.
   */
  double getDouble(final int row);

  /**
   * Returns the float value at the specified row in this column.
   *
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   * @throws UnsupportedOperationException if this column does not support this type.
   */
  float getFloat(final int row);

  /**
   * Returns the int value at the specified row in this column.
   *
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   * @throws UnsupportedOperationException if this column does not support this type.
   */
  int getInt(final int row);

  /**
   * Returns the long value at the specified row in this column.
   *
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   * @throws UnsupportedOperationException if this column does not support this type.
   */
  long getLong(final int row);

  /**
   * Returns the element at the specified row in this column.
   *
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   */
  @Nonnull
  Object getObject(final int row);

  /**
   * Returns the {@link String} value at the specified row in this column.
   *
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   * @throws UnsupportedOperationException if this column does not support this type.
   */
  @Nonnull
  String getString(final int row);
}
