package edu.washington.escience.myria.storage;

import java.io.Serializable;
import java.nio.BufferOverflowException;

import org.joda.time.DateTime;

import edu.washington.escience.myria.column.builder.WritableColumn;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * A field used in {@link Tuple}.
 *
 * @param <T> the type.
 */
public class Field<T extends Comparable<?>> implements WritableColumn, Serializable {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * The value of this field.
   */
  private Object value;

  @Override
  public WritableColumn appendBoolean(final boolean value) throws BufferOverflowException {
    this.value = value;
    return this;
  }

  @Override
  public WritableColumn appendDateTime(final DateTime value) throws BufferOverflowException {
    this.value = value;
    return this;
  }

  @Override
  public WritableColumn appendDouble(final double value) throws BufferOverflowException {
    this.value = value;
    return this;
  }

  @Override
  public WritableColumn appendFloat(final float value) throws BufferOverflowException {
    this.value = value;
    return this;
  }

  @Override
  public WritableColumn appendInt(final int value) throws BufferOverflowException {
    this.value = value;
    return this;
  }

  @Override
  public WritableColumn appendLong(final long value) throws BufferOverflowException {
    this.value = value;
    return this;
  }

  @Override
  public WritableColumn appendObject(final Object value) throws BufferOverflowException {
    this.value = MyriaUtils.ensureObjectIsValidType(value);
    return this;
  }

  @Override
  public WritableColumn appendString(final String value) throws BufferOverflowException {
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
