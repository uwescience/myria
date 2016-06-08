package edu.washington.escience.myria.column.builder;

import java.nio.BufferOverflowException;

import org.joda.time.DateTime;

/**
 *
 * An interface for column that can be written.
 */
public interface WritableColumn {

  /**
   * Inserts the specified element at end of this column.
   *
   * @param value element to be inserted.
   * @return this column.
   * @throws BufferOverflowException if exceeds buffer up bound.
   */
  WritableColumn appendBoolean(final boolean value) throws BufferOverflowException;

  /**
   * Inserts the specified element at end of this column.
   *
   * @param value element to be inserted.
   * @return this column.
   * @throws BufferOverflowException if exceeds buffer up bound.
   */
  WritableColumn appendDateTime(final DateTime value) throws BufferOverflowException;

  /**
   * Inserts the specified element at end of this column.
   *
   * @param value element to be inserted.
   * @return this column.
   * @throws BufferOverflowException if exceeds buffer up bound.
   */
  WritableColumn appendDouble(final double value) throws BufferOverflowException;

  /**
   * Inserts the specified element at end of this column.
   *
   * @param value element to be inserted.
   * @return this column.
   * @throws BufferOverflowException if exceeds buffer up bound.
   */
  WritableColumn appendFloat(final float value) throws BufferOverflowException;

  /**
   * Inserts the specified element at end of this column.
   *
   * @param value element to be inserted.
   * @return this column.
   * @throws BufferOverflowException if exceeds buffer up bound.
   */
  WritableColumn appendInt(final int value) throws BufferOverflowException;

  /**
   * Inserts the specified element at end of this column.
   *
   * @param value element to be inserted.
   * @return this column.
   * @throws BufferOverflowException if exceeds buffer up bound.
   */
  WritableColumn appendLong(final long value) throws BufferOverflowException;

  /**
   * Inserts the specified element at end of this column.
   *
   * @param value element to be inserted.
   * @return this column builder.
   * @throws BufferOverflowException if the column is already full
   */
  @Deprecated
  WritableColumn appendObject(final Object value) throws BufferOverflowException;

  /**
   * Inserts the specified element at end of this column.
   *
   * @param value element to be inserted.
   * @return this column.
   * @throws BufferOverflowException if exceeds buffer up bound.
   */
  WritableColumn appendString(final String value) throws BufferOverflowException;
}
