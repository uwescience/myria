package edu.washington.escience.myriad.column;

import java.nio.FloatBuffer;

import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.TupleBatch;

/**
 * A column of Float values.
 * 
 * @author dhalperi
 * 
 */
public final class FloatColumn implements Column {
  /** Internal representation of the column data. */
  private final FloatBuffer data;

  /** Constructs an empty column that can hold up to TupleBatch.BATCH_SIZE elements. */
  public FloatColumn() {
    this.data = FloatBuffer.allocate(TupleBatch.BATCH_SIZE);
  }

  @Override
  public Object get(final int row) {
    return Float.valueOf(getFloat(row));
  }

  /**
   * Returns the element at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   */
  public float getFloat(final int row) {
    Preconditions.checkElementIndex(row, data.position());
    return data.get(row);
  }

  @Override
  public void put(final Object value) {
    putFloat((Float) value);
  }

  /**
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   */
  public void putFloat(final float value) {
    Preconditions.checkElementIndex(data.position(), data.capacity());
    data.put(value);
  }

  @Override
  public int size() {
    return data.position();
  }
}