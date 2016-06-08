package edu.washington.escience.myria.column;

import com.google.common.base.Preconditions;

/**
 * A column of Int values.
 *
 *
 */
public final class IntArrayColumn extends IntColumn {
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
  public IntArrayColumn(final int[] data, final int numData) {
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
}
