package edu.washington.escience.myria.storage;

import org.joda.time.DateTime;

/**
 * An interface for a table that can have values appended to it.
 */
public interface AppendableTable {
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
  void putDateTime(final int column, final DateTime value);

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
  void putString(final int column, final String value);
}
