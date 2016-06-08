package edu.washington.escience.myria.column.builder;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.LongColumn;
import edu.washington.escience.myria.column.mutable.LongMutableColumn;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * A column of Long values.
 *
 */
public final class LongColumnBuilder extends ColumnBuilder<Long> {
  /** View of the column data as longs. */
  private final LongBuffer data;

  /**
   * If the builder has built the column.
   * */
  private boolean built = false;

  /** Constructs an empty column that can hold up to TupleBatch.BATCH_SIZE elements. */
  public LongColumnBuilder() {
    data = LongBuffer.allocate(TupleBatch.BATCH_SIZE);
  }

  /**
   * copy.
   *
   * @param data the underlying data
   * */
  private LongColumnBuilder(final LongBuffer data) {
    this.data = data;
  }

  /**
   * Constructs a LongColumn by deserializing the given ColumnMessage.
   *
   * @param message a ColumnMessage containing the contents of this column.
   * @param numTuples num tuples in the column message
   * @return the built column
   */
  public static LongColumn buildFromProtobuf(final ColumnMessage message, final int numTuples) {
    if (message.getType().ordinal() != ColumnMessage.Type.LONG_VALUE) {
      throw new IllegalArgumentException(
          "Trying to construct LongColumn from non-LONG ColumnMessage");
    }
    if (!message.hasLongColumn()) {
      throw new IllegalArgumentException("ColumnMessage has type LONG but no LongColumn");
    }
    ByteBuffer dataBytes = message.getLongColumn().getData().asReadOnlyByteBuffer();
    LongBuffer newData = LongBuffer.allocate(numTuples);
    for (int i = 0; i < numTuples; i++) {
      newData.put(dataBytes.getLong());
    }
    return new LongColumnBuilder(newData).build();
  }

  @Override
  public Type getType() {
    return Type.LONG_TYPE;
  }

  @Override
  public LongColumnBuilder appendLong(final long value) throws BufferOverflowException {
    Preconditions.checkArgument(
        !built, "No further changes are allowed after the builder has built the column.");
    data.put(value);
    return this;
  }

  @Override
  public ColumnBuilder<Long> appendFromJdbc(final ResultSet resultSet, final int jdbcIndex)
      throws SQLException, BufferOverflowException {
    Preconditions.checkArgument(
        !built, "No further changes are allowed after the builder has built the column.");
    return appendLong(resultSet.getLong(jdbcIndex));
  }

  @Override
  public ColumnBuilder<Long> appendFromSQLite(final SQLiteStatement statement, final int index)
      throws SQLiteException, BufferOverflowException {
    Preconditions.checkArgument(
        !built, "No further changes are allowed after the builder has built the column.");
    return appendLong(statement.columnLong(index));
  }

  @Deprecated
  @Override
  public ColumnBuilder<Long> appendObject(final Object value) throws BufferOverflowException {
    Preconditions.checkArgument(
        !built, "No further changes are allowed after the builder has built the column.");
    return appendLong((Long) MyriaUtils.ensureObjectIsValidType(value));
  }

  @Override
  public int size() {
    return data.position();
  }

  @Override
  public LongColumn build() {
    built = true;
    return new LongColumn(data.array(), data.position());
  }

  @Override
  public LongMutableColumn buildMutable() {
    built = true;
    return new LongMutableColumn(data.array(), data.position());
  }

  @Override
  public void replaceLong(final long value, final int row) throws IndexOutOfBoundsException {
    Preconditions.checkArgument(
        !built, "No further changes are allowed after the builder has built the column.");
    Preconditions.checkElementIndex(row, data.position());
    data.put(row, value);
  }

  @Override
  public ColumnBuilder<Long> expand(final int size) {
    Preconditions.checkArgument(
        !built, "No further changes are allowed after the builder has built the column.");
    Preconditions.checkArgument(size >= 0);
    data.position(data.position() + size);
    return this;
  }

  @Override
  public ColumnBuilder<Long> expandAll() {
    Preconditions.checkArgument(
        !built, "No further changes are allowed after the builder has built the column.");
    data.position(data.limit());
    return this;
  }

  @Override
  @Deprecated
  public Long getObject(final int row) {
    return data.get(row);
  }

  @Override
  public long getLong(final int row) {
    return data.get(row);
  }

  @Override
  public LongColumnBuilder forkNewBuilder() {
    long[] arr = new long[data.array().length];
    System.arraycopy(data.array(), 0, arr, 0, arr.length);
    return new LongColumnBuilder(
        (LongBuffer) LongBuffer.wrap(arr).position(data.position()).limit(data.limit()));
  }
}
