package edu.washington.escience.myria.storage;

import javax.annotation.Nonnull;

import org.joda.time.DateTime;

/**
 * An interface for a column in which values can be replaced or read.
 */
public interface ReplaceableColumn extends ReadableColumn {
  /**
   * Replaces the value at the specified row in this column.
   *
   * @param value the new value.
   * @param row row of element to replace.
   * @throws UnsupportedOperationException if this column does not support this type.
   */
  void replaceBoolean(final boolean value, final int row);

  /**
   * Replaces the value at the specified row in this column.
   *
   * @param value the new value.
   * @param row row of element to replace.
   * @throws UnsupportedOperationException if this column does not support this type.
   */
  void replaceDateTime(@Nonnull final DateTime value, final int row);

  /**
   * Replaces the value at the specified row in this column.
   *
   * @param value the new value.
   * @param row row of element to replace.
   * @throws UnsupportedOperationException if this column does not support this type.
   */
  void replaceDouble(final double value, final int row);

  /**
   * Replaces the value at the specified row in this column.
   *
   * @param value the new value.
   * @param row row of element to replace.
   * @throws UnsupportedOperationException if this column does not support this type.
   */
  void replaceFloat(final float value, final int row);

  /**
   * Replaces the value at the specified row in this column.
   *
   * @param value the new value.
   * @param row row of element to replace.
   * @throws UnsupportedOperationException if this column does not support this type.
   */
  void replaceInt(final int value, final int row);

  /**
   * Replaces the value at the specified row in this column.
   *
   * @param value the new value.
   * @param row row of element to replace.
   * @throws UnsupportedOperationException if this column does not support this type.
   */
  void replaceLong(final long value, final int row);

  /**
   * Replaces the value at the specified row in this column.
   *
   * @param value the new value.
   * @param row row of element to replace.
   * @throws UnsupportedOperationException if this column does not support this type.
   */
  void replaceString(@Nonnull final String value, final int row);
}
