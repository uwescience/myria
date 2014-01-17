package edu.washington.escience.myria.column.mutable;

import java.io.Serializable;

import org.joda.time.DateTime;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;

/**
 * A mutable column of a batch of tuples.
 * 
 * @param <T> type of the objects in this column.
 * 
 */
public abstract class MutableColumn<T extends Comparable<?>> implements Cloneable, Serializable {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * Returns the boolean value at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   * @throws UnsupportedOperationException if this column does not support this type.
   */
  public boolean getBoolean(final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * Returns the {@link DateTime} value at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   * @throws UnsupportedOperationException if this column does not support this type.
   */
  public DateTime getDateTime(final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * Returns the double value at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   * @throws UnsupportedOperationException if this column does not support this type.
   */
  public double getDouble(final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * Returns the float value at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   * @throws UnsupportedOperationException if this column does not support this type.
   */
  public float getFloat(final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * Returns the int value at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   * @throws UnsupportedOperationException if this column does not support this type.
   */
  public int getInt(final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * Returns the long value at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   * @throws UnsupportedOperationException if this column does not support this type.
   */
  public long getLong(final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * Returns the element at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   */
  @Deprecated
  public abstract T getObject(int row);

  /**
   * Returns the {@link String} value at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   * @throws UnsupportedOperationException if this column does not support this type.
   */
  public String getString(final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @return a Myria {@link Type} object explaining what type of data is in this column.
   */
  public abstract Type getType();

  /**
   * Returns the number of elements in this column.
   * 
   * @return the number of elements in this column.
   */
  public abstract int size();

  /**
   * replace the old value at index with the new value.
   * 
   * @param index the index
   * @param value the new value
   */
  public abstract void replace(int index, T value);

  /**
   * return the column representation of this mutable column. It copies data.
   * 
   * @return the column representation of this mutable column.
   */
  public abstract Column<T> toColumn();

  /**
   * clone itself by copying data.
   * 
   * @return the clone
   */
  @Override
  public abstract MutableColumn<T> clone();
}