package edu.washington.escience.myria.column.mutable;

import java.util.BitSet;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.BooleanColumn;
import edu.washington.escience.myria.util.ImmutableBitSet;

/**
 * A mutable column of Boolean values.
 *
 */
public class BooleanMutableColumn extends MutableColumn<Boolean> {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** Internal representation of the column data. */
  private final BitSet data;
  /** Number of valid elements. */
  private final int numBits;

  /**
   * @param data the data
   * @param size the size of this column;
   * */
  public BooleanMutableColumn(final BitSet data, final int size) {
    this.data = new ImmutableBitSet(data);
    numBits = size;
  }

  @Deprecated
  @Override
  public Boolean getObject(final int row) {
    return Boolean.valueOf(getBoolean(row));
  }

  @Override
  public boolean getBoolean(final int row) {
    Preconditions.checkElementIndex(row, numBits);
    return data.get(row);
  }

  @Override
  public Type getType() {
    return Type.BOOLEAN_TYPE;
  }

  @Override
  public int size() {
    return numBits;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(size()).append(" elements: [");
    for (int i = 0; i < size(); ++i) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(data.get(i));
    }
    sb.append(']');
    return sb.toString();
  }

  @Override
  public void replaceBoolean(final boolean value, final int row) {
    Preconditions.checkElementIndex(row, size());
    data.set(row, value);
  }

  @Override
  public BooleanColumn toColumn() {
    return new BooleanColumn((BitSet) data.clone(), numBits);
  }

  @Override
  public BooleanMutableColumn clone() {
    return new BooleanMutableColumn((BitSet) data.clone(), numBits);
  }
}
