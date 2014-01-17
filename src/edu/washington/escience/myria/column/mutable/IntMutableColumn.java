package edu.washington.escience.myria.column.mutable;

import edu.washington.escience.myria.Type;

/**
 * An abstract MutableColumn<Integer> with a primitive type accessor.
 * 
 */
public abstract class IntMutableColumn extends MutableColumn<Integer> {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * Returns the element at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   */
  @Override
  public abstract int getInt(final int row);

  @Override
  public final Type getType() {
    return Type.INT_TYPE;
  }

  @Override
  public final String toString() {
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
  public abstract IntMutableColumn clone();
}
