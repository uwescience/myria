package edu.washington.escience.myria.column.mutable;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.DoubleColumn;

/**
 * A mutable column of Double values.
 * 
 */
public final class DoubleMutableColumn extends MutableColumn<Double> {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** Internal representation of the column data. */
  private final double[] data;
  /** The number of existing rows in this column. */
  private final int position;

  /**
   * Constructs a new column.
   * 
   * @param data the data
   * @param numData number of tuples.
   * */
  public DoubleMutableColumn(final double[] data, final int numData) {
    Preconditions.checkNotNull(data);
    Preconditions.checkArgument(numData <= TupleBatch.BATCH_SIZE);
    this.data = data;
    position = numData;
  }

  @Override
  public Double getObject(final int row) {
    return Double.valueOf(getDouble(row));
  }

  @Override
  public double getDouble(final int row) {
    Preconditions.checkElementIndex(row, position);
    return data[row];
  }

  @Override
  public Type getType() {
    return Type.DOUBLE_TYPE;
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
  public void replace(final int index, final Double value) {
    replace(index, value.doubleValue());
  }

  /**
   * replace the value on a row with the given double value.
   * 
   * @param index row index
   * @param value the double value.
   */
  public void replace(final int index, final double value) {
    Preconditions.checkElementIndex(index, size());
    data[index] = value;
  }

  @Override
  public DoubleColumn toColumn() {
    return new DoubleColumn(data.clone(), position);
  }

  @Override
  public DoubleMutableColumn clone() {
    return new DoubleMutableColumn(data.clone(), position);
  }
}