package edu.washington.escience.myria.column.mutable;

import java.io.Serializable;

import javax.annotation.Nonnull;

import org.joda.time.DateTime;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.storage.ReadableColumn;
import edu.washington.escience.myria.storage.ReplaceableColumn;

/**
 * A mutable column of a batch of tuples.
 *
 * @param <T> type of the objects in this column.
 *
 */
public abstract class MutableColumn<T extends Comparable<?>>
    implements Cloneable, ReadableColumn, ReplaceableColumn, Serializable {

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

  @Override
  public void replaceBoolean(final boolean value, final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public void replaceDateTime(@Nonnull final DateTime value, final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public void replaceDouble(final double value, final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public void replaceFloat(final float value, final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public void replaceInt(final int value, final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public void replaceLong(final long value, final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public void replaceString(@Nonnull final String value, final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * return the column representation of this mutable column. It copies data.
   *
   * @return the column representation of this mutable column.
   */
  public abstract Column<T> toColumn();

  @Override
  public abstract MutableColumn<T> clone();
}
