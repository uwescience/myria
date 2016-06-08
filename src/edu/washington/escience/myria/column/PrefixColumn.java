package edu.washington.escience.myria.column;

import javax.annotation.Nonnull;

import org.joda.time.DateTime;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Type;

/**
 * Provides the abstraction of a full Column that is a prefix of an existing column.
 *
 * @param <T> the type of the inner Column.
 */
public class PrefixColumn<T extends Comparable<?>> extends Column<T> {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** Number of rows. */
  private final int numRows;
  /** The inner column. */
  private final Column<T> inner;

  /**
   * Creates a prefix of rows in the specified inner column.
   *
   * @param inner the {@link Column} to be prefixed.
   * @param length the number of rows of the inner Column to be retained.
   */
  public PrefixColumn(@Nonnull final Column<T> inner, final int length) {
    this.inner = inner;
    Preconditions.checkArgument(
        length <= inner.size(),
        "Error: cannot take a prefix of length %s from a batch of length %s",
        length,
        inner.size());
    numRows = length;
  }

  @Override
  public boolean getBoolean(final int row) {
    return inner.getBoolean(Preconditions.checkElementIndex(row, numRows));
  }

  @Override
  public DateTime getDateTime(final int row) {
    return inner.getDateTime(Preconditions.checkElementIndex(row, numRows));
  }

  @Override
  public double getDouble(final int row) {
    return inner.getDouble(Preconditions.checkElementIndex(row, numRows));
  }

  @Override
  public float getFloat(final int row) {
    return inner.getFloat(Preconditions.checkElementIndex(row, numRows));
  }

  @Override
  public int getInt(final int row) {
    return inner.getInt(Preconditions.checkElementIndex(row, numRows));
  }

  @Override
  public long getLong(final int row) {
    return inner.getLong(Preconditions.checkElementIndex(row, numRows));
  }

  @Override
  public String getString(final int row) {
    return inner.getString(Preconditions.checkElementIndex(row, numRows));
  }

  @Override
  public T getObject(final int row) {
    return inner.getObject(Preconditions.checkElementIndex(row, numRows));
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
