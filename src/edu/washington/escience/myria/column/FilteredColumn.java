package edu.washington.escience.myria.column;

import java.util.BitSet;

import org.joda.time.DateTime;

import edu.washington.escience.myria.Type;

/**
 * Provides the abstraction of a full Column filtered to an indicated set of rows.
 *
 * @param <T> the type of the inner Column.
 */
class FilteredColumn<T extends Comparable<?>> extends Column<T> {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** Number of rows. */
  private final int numRows;
  /** Which the indices of valid rows. */
  private final int[] validIndices;
  /** The inner column. */
  private final Column<T> inner;

  /**
   * Creates a filtered version of the indicated rows in the specified inner column.
   *
   * @param inner the {@link Column} to be filtered.
   * @param indices the rows of the inner Column to be kept.
   */
  protected FilteredColumn(final Column<T> inner, final int[] indices) {
    this.inner = inner;
    numRows = indices.length;
    validIndices = indices;
  }

  /**
   * Creates a filtered version of the indicated rows in the specified inner column.
   *
   * @param inner the {@link Column} to be filtered.
   * @param filter a BitSet indicating the rows of the inner Column to be kept.
   */
  protected FilteredColumn(final Column<T> inner, final BitSet filter) {
    this.inner = inner;
    numRows = filter.cardinality();
    int[] indices = new int[numRows];
    int rowCount = 0;
    for (int i = filter.nextSetBit(0); i >= 0; i = filter.nextSetBit(i + 1)) {
      indices[rowCount] = i;
      rowCount++;
    }
    validIndices = indices;
  }

  /**
   * Helper function to convert external row number into internal row number.
   *
   * @param row the external row number
   * @return the row number in the inner, filtered column.
   */
  private int convertRow(final int row) {
    return validIndices[row];
  }

  @Override
  public boolean getBoolean(final int row) {
    return inner.getBoolean(convertRow(row));
  }

  @Override
  public DateTime getDateTime(final int row) {
    return inner.getDateTime(convertRow(row));
  }

  @Override
  public double getDouble(final int row) {
    return inner.getDouble(convertRow(row));
  }

  @Override
  public float getFloat(final int row) {
    return inner.getFloat(convertRow(row));
  }

  @Override
  public int getInt(final int row) {
    return inner.getInt(convertRow(row));
  }

  @Override
  public long getLong(final int row) {
    return inner.getLong(convertRow(row));
  }

  @Override
  public String getString(final int row) {
    return inner.getString(convertRow(row));
  }

  @Override
  public T getObject(final int row) {
    return inner.getObject(convertRow(row));
  }

  @Override
  public Type getType() {
    return inner.getType();
  }

  @Override
  public int size() {
    return numRows;
  }
}
