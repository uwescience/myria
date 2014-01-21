package edu.washington.escience.myria.column.builder;

import java.nio.BufferOverflowException;

import org.joda.time.DateTime;

/**
 * 
 * An interface for column that can be written.
 * 
 * @param <T> The type of the column.
 */
public interface WritableColumn<T extends Comparable<?>> {

  /**
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   * @return this column.
   * @throws BufferOverflowException if exceeds buffer up bound.
   */
  WritableColumn<T> appendBoolean(final boolean value) throws BufferOverflowException;

  /**
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   * @return this column.
   * @throws BufferOverflowException if exceeds buffer up bound.
   */
  WritableColumn<T> appendDateTime(final DateTime value) throws BufferOverflowException;

  /**
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   * @return this column.
   * @throws BufferOverflowException if exceeds buffer up bound.
   */
  WritableColumn<T> appendDouble(final double value) throws BufferOverflowException;

  /**
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   * @return this column.
   * @throws BufferOverflowException if exceeds buffer up bound.
   */
  WritableColumn<T> appendFloat(final float value) throws BufferOverflowException;

  /**
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   * @return this column.
   * @throws BufferOverflowException if exceeds buffer up bound.
   */
  WritableColumn<T> appendInt(final int value) throws BufferOverflowException;

  /**
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   * @return this column.
   * @throws BufferOverflowException if exceeds buffer up bound.
   */
  WritableColumn<T> appendLong(final long value) throws BufferOverflowException;

  /**
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   * @return this column builder.
   * @throws BufferOverflowException if the column is already full
   */
  @Deprecated
  WritableColumn<T> appendObject(final Object value) throws BufferOverflowException;

  /**
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   * @return this column.
   * @throws BufferOverflowException if exceeds buffer up bound.
   */
  WritableColumn<T> appendString(final String value) throws BufferOverflowException;

}