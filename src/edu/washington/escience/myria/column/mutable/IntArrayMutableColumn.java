package edu.washington.escience.myria.column.mutable;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.column.IntArrayColumn;

/**
 * A mutable column of Int values.
 * 
 */
public final class IntArrayMutableColumn extends IntMutableColumn {
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  /** Internal representation of the column data. */
  private final int[] data;
  /** The number of existing rows in this column. */
  private final int position;

  /**
   * Constructs a new column.
   * 
   * @param data the data
   * @param numData number of tuples.
   * */
  public IntArrayMutableColumn(final int[] data, final int numData) {
    this.data = data;
    position = numData;
  }

  @Override
  public Integer getObject(final int row) {
    return Integer.valueOf(getInt(row));
  }

  @Override
  public int getInt(final int row) {
    Preconditions.checkElementIndex(row, position);
    return data[row];
  }

  @Override
  public int size() {
    return position;
  }

  @Override
  public void replace(final int index, final Integer value) {
    replace(index, value.intValue());
  }

  /**
   * replace the value on a row with the given int value.
   * 
   * @param index row index
   * @param value the int value.
   */
  public void replace(final int index, final int value) {
    Preconditions.checkElementIndex(index, size());
    data[index] = value;
  }

  @Override
  public IntArrayColumn toColumn() {
    return new IntArrayColumn(data.clone(), position);
  }

  @Override
  public IntArrayMutableColumn clone() {
    return new IntArrayMutableColumn(data.clone(), position);
  }
}