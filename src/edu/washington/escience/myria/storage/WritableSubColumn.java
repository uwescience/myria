package edu.washington.escience.myria.storage;

import java.nio.BufferOverflowException;
import java.util.Objects;

import org.joda.time.DateTime;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.column.builder.WritableColumn;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * A trivial wrapper to expose a single column of a {@link WritableTable}.
 */
public final class WritableSubColumn implements WritableColumn {
  /** The wrapped {@link WritableTable}. */
  private final AppendableTable inner;
  /** The column in {@link #inner} exposed. */
  private final int column;

  /**
   * Constructs a wrapper to present the specified column of the given table as a {@link WritableColumn}.
   *
   * @param table the table to be wrapped
   * @param column which column this object represents
   */
  public WritableSubColumn(final AppendableTable table, final int column) {
    inner = Objects.requireNonNull(table, "inner");
    this.column = Preconditions.checkElementIndex(column, table.numColumns());
  }

  @Override
  public WritableColumn appendBoolean(final boolean value) throws BufferOverflowException {
    inner.putBoolean(column, value);
    return this;
  }

  @Override
  public WritableColumn appendDateTime(final DateTime value) throws BufferOverflowException {
    inner.putDateTime(column, value);
    return this;
  }

  @Override
  public WritableColumn appendDouble(final double value) throws BufferOverflowException {
    inner.putDouble(column, value);
    return this;
  }

  @Override
  public WritableColumn appendFloat(final float value) throws BufferOverflowException {
    inner.putFloat(column, value);
    return this;
  }

  @Override
  public WritableColumn appendInt(final int value) throws BufferOverflowException {
    inner.putInt(column, value);
    return this;
  }

  @Override
  public WritableColumn appendLong(final long value) throws BufferOverflowException {
    inner.putLong(column, value);
    return this;
  }

  @Override
  @Deprecated
  public WritableColumn appendObject(final Object value) throws BufferOverflowException {
    inner.putObject(column, MyriaUtils.ensureObjectIsValidType(value));
    return this;
  }

  @Override
  public WritableColumn appendString(final String value) throws BufferOverflowException {
    inner.putString(column, value);
    return this;
  }
}
