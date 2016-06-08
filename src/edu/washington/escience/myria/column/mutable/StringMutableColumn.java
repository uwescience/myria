package edu.washington.escience.myria.column.mutable;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.StringArrayColumn;
import edu.washington.escience.myria.column.StringColumn;

/**
 * A mutable column of String values.
 *
 */
public final class StringMutableColumn extends MutableColumn<String> {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** Contains the packed character data. */
  private final String[] data;
  /** Number of elements in this column. */
  private final int numStrings;

  /**
   * Constructs a new column.
   *
   * @param data the data
   * @param numStrings number of tuples.
   * */
  public StringMutableColumn(final String[] data, final int numStrings) {
    this.data = data;
    this.numStrings = numStrings;
  }

  @Deprecated
  @Override
  public String getObject(final int row) {
    return getString(row);
  }

  @Override
  public String getString(final int row) {
    Preconditions.checkElementIndex(row, numStrings);
    return data[row];
  }

  @Override
  public Type getType() {
    return Type.STRING_TYPE;
  }

  @Override
  public int size() {
    return numStrings;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(size()).append(" elements: [");
    for (int i = 0; i < size(); ++i) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(getString(i));
    }
    sb.append(']');
    return sb.toString();
  }

  @Override
  public void replaceString(final String value, final int row) {
    Preconditions.checkElementIndex(row, size());
    data[row] = value;
  }

  @Override
  public StringColumn toColumn() {
    return new StringArrayColumn(data.clone(), numStrings);
  }

  @Override
  public StringMutableColumn clone() {
    return new StringMutableColumn(data.clone(), numStrings);
  }
}
