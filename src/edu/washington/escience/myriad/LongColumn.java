package edu.washington.escience.myriad;

import java.nio.LongBuffer;

/**
 * A column of Long values.
 * 
 * @author dhalperi
 * 
 */
public final class LongColumn extends Column {
  /** Internal representation of the column data. */
  private final LongBuffer data;

  /** Constructs an empty column that can hold up to TupleBatch.BATCH_SIZE elements. */
  public LongColumn() {
    this.data = LongBuffer.allocate(TupleBatch.BATCH_SIZE);
  }

  @Override
  public Object get(final int row) {
    return Long.valueOf(getLong(row));
  }

  /**
   * Returns the element at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   */
  public long getLong(final int row) {
    return data.get(row);
  }

  @Override
  protected void put(final Object value) {
    putLong((Long) value);
  }

  /**
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   */
  public void putLong(final long value) {
    data.put(value);
  }

  @Override
  public int size() {
    return data.position();
  }
}