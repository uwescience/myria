package edu.washington.escience.myriad;

import java.nio.IntBuffer;

import com.google.common.base.Preconditions;

/**
 * A column of Integer values.
 * 
 * @author dhalperi
 * 
 */
public final class IntColumn extends Column {
  /** Internal representation of the column data. */
  private final IntBuffer data;

  /** Constructs an empty column that can hold up to TupleBatch.BATCH_SIZE elements. */
  public IntColumn() {
    this.data = IntBuffer.allocate(TupleBatch.BATCH_SIZE);
  }

  @Override
  public Object get(final int row) {
    Preconditions.checkElementIndex(row, data.position());
    return Integer.valueOf(data.get(row));
  }

  /**
   * Returns the element at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   */
  public int getInt(final int row) {
    return data.get(row);
  }

  @Override
  protected void put(final Object value) {
    putInt((Integer) value);
  }

  /**
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   */
  public void putInt(final int value) {
    data.put(value);
  }

  @Override
  public int size() {
    return data.position();
  }
}