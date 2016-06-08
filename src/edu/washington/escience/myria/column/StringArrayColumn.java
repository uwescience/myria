package edu.washington.escience.myria.column;

import com.google.common.base.Preconditions;

/**
 * A column of String values.
 *
 *
 */
public final class StringArrayColumn extends StringColumn {
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
  public StringArrayColumn(final String[] data, final int numStrings) {
    this.data = data;
    this.numStrings = numStrings;
  }

  /**
   * Returns the element at the specified row in this column.
   *
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   */
  @Override
  public String getString(final int row) {
    return data[Preconditions.checkElementIndex(row, numStrings)];
  }

  @Override
  public int size() {
    return numStrings;
  }
}
