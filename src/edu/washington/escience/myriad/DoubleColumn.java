package edu.washington.escience.myriad;

import java.nio.DoubleBuffer;

import com.google.common.base.Preconditions;

/**
 * A column of Double values.
 * 
 * @author dhalperi
 * 
 */
public final class DoubleColumn extends Column {
  /** Internal representation of the column data. */
  private final DoubleBuffer data;

  /** Constructs an empty column that can hold up to TupleBatch.BATCH_SIZE elements. */
  public DoubleColumn() {
    this.data = DoubleBuffer.allocate(TupleBatch.BATCH_SIZE);
  }

  @Override
  public Object get(final int row) {
    return Double.valueOf(getDouble(row));
  }

  /**
   * Returns the element at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   */
  public double getDouble(final int row) {
    Preconditions.checkElementIndex(row, data.position());
    return data.get(row);
  }

  @Override
  protected void put(final Object value) {
    putDouble((Double) value);
  }

  /**
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   */
  public void putDouble(final double value) {
    Preconditions.checkElementIndex(data.position(), data.capacity());
    data.put(value);
  }

  @Override
  public int size() {
    return data.position();
  }
}