package edu.washington.escience.myria.storage;

import java.util.Objects;

import org.joda.time.DateTime;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Type;

/**
 * A trivial wrapper to expose a single column of a {@link ReadableTable}.
 */
public final class ReadableSubColumn implements ReadableColumn {
  /** The wrapped {@link ReadableTable}. */
  private final ReadableTable inner;
  /** The column in {@link #inner} exposed. */
  private final int column;

  /**
   * Constructs a wrapper to present the specified column of the given table as a {@link ReadableColumn}.
   *
   * @param table the table to be wrapped
   * @param column which column this object represents
   */
  public ReadableSubColumn(final ReadableTable table, final int column) {
    inner = Objects.requireNonNull(table, "inner");
    this.column = Preconditions.checkElementIndex(column, table.getSchema().numColumns());
  }

  @Override
  public Type getType() {
    return inner.getSchema().getColumnType(column);
  }

  @Override
  public int size() {
    return inner.numTuples();
  }

  @Override
  public boolean getBoolean(final int row) {
    return inner.getBoolean(column, row);
  }

  @Override
  public DateTime getDateTime(final int row) {
    return inner.getDateTime(column, row);
  }

  @Override
  public double getDouble(final int row) {
    return inner.getDouble(column, row);
  }

  @Override
  public float getFloat(final int row) {
    return inner.getFloat(column, row);
  }

  @Override
  public int getInt(final int row) {
    return inner.getInt(column, row);
  }

  @Override
  public long getLong(final int row) {
    return inner.getLong(column, row);
  }

  @Override
  @Deprecated
  public Object getObject(final int row) {
    return inner.getObject(column, row);
  }

  @Override
  public String getString(final int row) {
    return inner.getString(column, row);
  }
}
