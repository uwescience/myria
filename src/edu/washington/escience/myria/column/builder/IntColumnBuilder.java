package edu.washington.escience.myria.column.builder;

import java.nio.BufferOverflowException;
import java.nio.IntBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.IntArrayColumn;
import edu.washington.escience.myria.column.IntColumn;
import edu.washington.escience.myria.column.IntProtoColumn;
import edu.washington.escience.myria.column.mutable.IntMutableColumn;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * A column of Integer values.
 *
 */
public final class IntColumnBuilder extends ColumnBuilder<Integer> {
  /** View of the column data as ints. */
  private final IntBuffer data;

  /**
   * If the builder has built the column.
   * */
  private boolean built = false;

  /** Constructs an empty column that can hold up to TupleBatch.BATCH_SIZE elements. */
  public IntColumnBuilder() {
    data = IntBuffer.allocate(TupleBatch.BATCH_SIZE);
  }

  /**
   * copy.
   *
   * @param data the underlying data
   * */
  private IntColumnBuilder(final IntBuffer data) {
    this.data = data;
  }

  /**
   * Constructs an IntColumn by deserializing the given ColumnMessage.
   *
   * @param message a ColumnMessage containing the contents of this column.
   * @param numTuples num tuples in the column message
   * @return the built column
   */
  public static IntColumn buildFromProtobuf(final ColumnMessage message, final int numTuples) {
    if (message.getType().ordinal() != ColumnMessage.Type.INT_VALUE) {
      throw new IllegalArgumentException(
          "Trying to construct IntColumn from non-INT ColumnMessage");
    }
    if (!message.hasIntColumn()) {
      throw new IllegalArgumentException("ColumnMessage has type INT but no IntColumn");
    }
    return new IntProtoColumn(message.getIntColumn());
  }

  @Override
  public Type getType() {
    return Type.INT_TYPE;
  }

  @Override
  public IntColumnBuilder appendInt(final int value) throws BufferOverflowException {
    Preconditions.checkArgument(
        !built, "No further changes are allowed after the builder has built the column.");
    data.put(value);
    return this;
  }

  @Deprecated
  @Override
  public IntColumnBuilder appendObject(final Object value) throws BufferOverflowException {
    Preconditions.checkArgument(
        !built, "No further changes are allowed after the builder has built the column.");
    return appendInt((Integer) MyriaUtils.ensureObjectIsValidType(value));
  }

  @Override
  public IntColumnBuilder appendFromJdbc(final ResultSet resultSet, final int jdbcIndex)
      throws SQLException, BufferOverflowException {
    Preconditions.checkArgument(
        !built, "No further changes are allowed after the builder has built the column.");
    return appendInt(resultSet.getInt(jdbcIndex));
  }

  @Override
  public IntColumnBuilder appendFromSQLite(final SQLiteStatement statement, final int index)
      throws SQLiteException, BufferOverflowException {
    Preconditions.checkArgument(
        !built, "No further changes are allowed after the builder has built the column.");
    return appendInt(statement.columnInt(index));
  }

  @Override
  public int size() {
    return data.position();
  }

  @Override
  public IntColumn build() {
    built = true;
    return new IntArrayColumn(data.array(), data.position());
  }

  @Override
  public IntMutableColumn buildMutable() {
    built = true;
    return new IntMutableColumn(data.array(), data.position());
  }

  @Override
  public void replaceInt(final int value, final int row) throws IndexOutOfBoundsException {
    Preconditions.checkArgument(
        !built, "No further changes are allowed after the builder has built the column.");
    Preconditions.checkElementIndex(row, data.position());
    data.put(row, value);
  }

  @Override
  public IntColumnBuilder expand(final int size) {
    Preconditions.checkArgument(
        !built, "No further changes are allowed after the builder has built the column.");
    Preconditions.checkArgument(size >= 0);
    data.position(data.position() + size);
    return this;
  }

  @Override
  public IntColumnBuilder expandAll() {
    Preconditions.checkArgument(
        !built, "No further changes are allowed after the builder has built the column.");
    data.position(data.capacity());
    return this;
  }

  @Override
  @Deprecated
  public Integer getObject(final int row) {
    return data.get(row);
  }

  @Override
  public int getInt(final int row) {
    return data.get(row);
  }

  @Override
  public IntColumnBuilder forkNewBuilder() {
    int[] arr = new int[data.array().length];
    System.arraycopy(data.array(), 0, arr, 0, data.position());
    return new IntColumnBuilder(
        (IntBuffer) IntBuffer.wrap(arr).position(data.position()).limit(data.limit()));
  }
}
