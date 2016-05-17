package edu.washington.escience.myria.column;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Type;

/**
 * A column of Long values.
 *
 *
 */
public final class LongColumn extends Column<Long> {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** Internal representation of the column data. */
  private final long[] data;
  /** The number of existing rows in this column. */
  private final int position;

  /**
   * Constructs a new column.
   *
   * @param data the data
   * @param numData number of tuples.
   * */
  public LongColumn(final long[] data, final int numData) {
    this.data = data;
    position = numData;
  }

  @Override
  public Long getObject(final int row) {
    return Long.valueOf(getLong(row));
  }

  @Override
  public long getLong(final int row) {
    Preconditions.checkElementIndex(row, position);
    return data[row];
  }

  @Override
  public Type getType() {
    return Type.LONG_TYPE;
  }

  @Override
  public int size() {
    return position;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(size()).append(" elements: [");
    for (int i = 0; i < size(); ++i) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(data[i]);
    }
    sb.append(']');
    return sb.toString();
  }
}
