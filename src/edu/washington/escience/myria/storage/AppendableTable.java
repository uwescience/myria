package edu.washington.escience.myria.storage;

import javax.annotation.Nonnull;

import org.joda.time.DateTime;

import edu.washington.escience.myria.column.builder.WritableColumn;

/**
 * An interface for a table that can have values appended to it.
 */
public interface AppendableTable extends TupleTable {
  /**
   * Append the specified value to the specified column.
   *
   * @param column index of the column.
   * @param value value to be appended.
   */
  void putBoolean(final int column, final boolean value);

  /**
   * Append the specified value to the specified column.
   *
   * @param column index of the column.
   * @param value value to be appended.
   */
  void putDateTime(final int column, @Nonnull final DateTime value);

  /**
   * Append the specified value to the specified column.
   *
   * @param column index of the column.
   * @param value value to be appended.
   */
  void putDouble(final int column, final double value);

  /**
   * Append the specified value to the specified column.
   *
   * @param column index of the column.
   * @param value value to be appended.
   */
  void putFloat(final int column, final float value);

  /**
   * Append the specified value to the specified column.
   *
   * @param column index of the column.
   * @param value value to be appended.
   */
  void putInt(final int column, final int value);

  /**
   * Append the specified value to the specified column.
   *
   * @param column index of the column.
   * @param value value to be appended.
   */
  void putLong(final int column, final long value);

  /**
   * Append the specified value to the specified column.
   *
   * @param column index of the column.
   * @param value value to be appended.
   */
  void putString(final int column, @Nonnull final String value);

  /**
   * Append the specified value to the specified column.
   *
   * @param column index of the column.
   * @param value value to be appended.
   */
  @Deprecated
  void putObject(final int column, @Nonnull final Object value);

  /**
   * @param column the index of the column to be returned.
   * @return a {@link ReadableColumn} representation of the specified column of this table.
   */
  @Nonnull
  WritableColumn asWritableColumn(final int column);
}
