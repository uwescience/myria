package edu.washington.escience.myria.column.mutable;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.IntArrayColumn;

/**
 * A mutable column of Int values.
 *
 */
public final class IntMutableColumn extends MutableColumn<Integer> {
  /** Required for Java serialization. */
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
  public IntMutableColumn(final int[] data, final int numData) {
    this.data = data;
    position = numData;
  }

  @Deprecated
  @Override
  public Integer getObject(final int row) {
    return Integer.valueOf(getInt(row));
  }

  @Override
  public Type getType() {
    return Type.INT_TYPE;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(size()).append(" elements: [");
    for (int i = 0; i < size(); ++i) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(getInt(i));
    }
    sb.append(']');
    return sb.toString();
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
  public void replaceInt(final int value, final int row) {
    Preconditions.checkElementIndex(row, size());
    data[row] = value;
  }

  @Override
  public IntArrayColumn toColumn() {
    return new IntArrayColumn(data.clone(), position);
  }

  @Override
  public IntMutableColumn clone() {
    return new IntMutableColumn(data.clone(), position);
  }
}
