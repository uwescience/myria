package edu.washington.escience.myria;

import java.io.Serializable;
import java.nio.BufferOverflowException;

import org.joda.time.DateTime;

import edu.washington.escience.myria.column.builder.WritableColumn;

/**
 * A field used in {@link Tuple}.
 * 
 * @param <T> the type.
 */
public class Field<T extends Comparable<?>> implements WritableColumn<T>, Serializable {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * The value of this field.
   */
  private Object value;

  @Override
  public WritableColumn<T> appendBoolean(final boolean value) throws BufferOverflowException {
    this.value = value;
    return this;
  }

  @Override
  public WritableColumn<T> appendDateTime(final DateTime value) throws BufferOverflowException {
    this.value = value;
    return this;
  }

  @Override
  public WritableColumn<T> appendDouble(final double value) throws BufferOverflowException {
    this.value = value;
    return this;
  }

  @Override
  public WritableColumn<T> appendFloat(final float value) throws BufferOverflowException {
    this.value = value;
    return this;
  }

  @Override
  public WritableColumn<T> appendInt(final int value) throws BufferOverflowException {
    this.value = value;
    return this;
  }

  @Override
  public WritableColumn<T> appendLong(final long value) throws BufferOverflowException {
    this.value = value;
    return this;
  }

  @Override
  public WritableColumn<T> appendObject(final Object value) throws BufferOverflowException {
    this.value = value;
    return this;
  }

  @Override
  public WritableColumn<T> appendString(final String value) throws BufferOverflowException {
    this.value = value;
    return this;
  }

  /**
   * @return the value
   */
  public Object getObject() {
    return value;
  }
}
