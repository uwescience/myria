package edu.washington.escience.myria.column.mutable;

import java.io.Serializable;

import org.joda.time.DateTime;

import edu.washington.escience.myria.ReadableColumn;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;

/**
 * A mutable column of a batch of tuples.
 * 
 * @param <T> type of the objects in this column.
 * 
 */
public abstract class MutableColumn<T extends Comparable<?>> implements Cloneable, ReadableColumn, Serializable {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  @Override
  public boolean getBoolean(final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public DateTime getDateTime(final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public double getDouble(final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public float getFloat(final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public int getInt(final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public long getLong(final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Deprecated
  @Override
  public abstract T getObject(int row);

  @Override
  public String getString(final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public abstract Type getType();

  @Override
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

  @Override
  public abstract MutableColumn<T> clone();
}