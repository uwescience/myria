package edu.washington.escience.myria.column.mutable;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.FloatColumn;

/**
 * A mutable column of Float values.
 * 
 */
public final class FloatMutableColumn extends MutableColumn<Float> {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** Internal representation of the column data. */
  private final float[] data;
  /** The number of existing rows in this column. */
  private final int position;

  /**
   * Constructs a new column.
   * 
   * @param data the underlying data
   * @param numData number of tuples.
   * */
  public FloatMutableColumn(final float[] data, final int numData) {
    Preconditions.checkNotNull(data);
    Preconditions.checkArgument(numData <= TupleBatch.BATCH_SIZE);
    this.data = data;
    position = numData;
  }

  @Deprecated
  @Override
  public Float getObject(final int row) {
    return Float.valueOf(getFloat(row));
  }

  @Override
  public float getFloat(final int row) {
    Preconditions.checkElementIndex(row, position);
    return data[row];
  }

  @Override
  public Type getType() {
    return Type.FLOAT_TYPE;
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

  @Override
  public void replace(final int index, final Float value) {
    replace(index, value.floatValue());
  }

  /**
   * replace the value on a row with the given float value.
   * 
   * @param index row index
   * @param value the float value.
   */
  public void replace(final int index, final float value) {
    Preconditions.checkElementIndex(index, size());
    data[index] = value;
  }

  @Override
  public FloatColumn toColumn() {
    return new FloatColumn(data.clone(), position);
  }

  @Override
  public FloatMutableColumn clone() {
    return new FloatMutableColumn(data.clone(), position);
  }
}