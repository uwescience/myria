package edu.washington.escience.myria.column.builder;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.DoubleColumn;
import edu.washington.escience.myria.column.mutable.DoubleMutableColumn;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * A column of Double values.
 *
 */
public final class DoubleColumnBuilder extends ColumnBuilder<Double> {
  /** View of the column data as doubles. */
  private final DoubleBuffer data;

  /**
   * If the builder has built the column.
   * */
  private boolean built = false;

  /** Constructs an empty column that can hold up to TupleBatch.BATCH_SIZE elements. */
  public DoubleColumnBuilder() {
    data = DoubleBuffer.allocate(TupleBatch.BATCH_SIZE);
  }

  /**
   * copy.
   *
   * @param data the underlying data
   * */
  private DoubleColumnBuilder(final DoubleBuffer data) {
    this.data = data;
  }

  /**
   * Constructs a DoubleColumn by deserializing the given ColumnMessage.
   *
   * @param message a ColumnMessage containing the contents of this column.
   * @param numTuples num tuples in the column message
   * @return the built column
   */
  public static DoubleColumn buildFromProtobuf(final ColumnMessage message, final int numTuples) {
    if (message.getType().ordinal() != ColumnMessage.Type.DOUBLE_VALUE) {
      throw new IllegalArgumentException(
          "Trying to construct DoubleColumn from non-DOUBLE ColumnMessage");
    }
    if (!message.hasDoubleColumn()) {
      throw new IllegalArgumentException("ColumnMessage has type DOUBLE but no DoubleColumn");
    }
    ByteBuffer dataBytes = message.getDoubleColumn().getData().asReadOnlyByteBuffer();
    DoubleBuffer newData = DoubleBuffer.allocate(numTuples);
    for (int i = 0; i < numTuples; i++) {
      newData.put(dataBytes.getDouble());
    }
    return new DoubleColumnBuilder(newData).build();
  }

  @Override
  public Type getType() {
    return Type.DOUBLE_TYPE;
  }

  @Override
  public DoubleColumnBuilder appendDouble(final double value) throws BufferOverflowException {
    Preconditions.checkArgument(
        !built, "No further changes are allowed after the builder has built the column.");
    data.put(value);
    return this;
  }

  @Deprecated
  @Override
  public DoubleColumnBuilder appendObject(final Object value) throws BufferOverflowException {
    Preconditions.checkArgument(
        !built, "No further changes are allowed after the builder has built the column.");
    return appendDouble((Double) MyriaUtils.ensureObjectIsValidType(value));
  }

  @Override
  public DoubleColumnBuilder appendFromJdbc(final ResultSet resultSet, final int jdbcIndex)
      throws SQLException, BufferOverflowException {
    Preconditions.checkArgument(
        !built, "No further changes are allowed after the builder has built the column.");
    return appendDouble(resultSet.getDouble(jdbcIndex));
  }

  @Override
  public DoubleColumnBuilder appendFromSQLite(final SQLiteStatement statement, final int index)
      throws SQLiteException, BufferOverflowException {
    Preconditions.checkArgument(
        !built, "No further changes are allowed after the builder has built the column.");
    return appendDouble(statement.columnDouble(index));
  }

  @Override
  public int size() {
    return data.position();
  }

  @Override
  public DoubleColumnBuilder expandAll() {
    Preconditions.checkArgument(
        !built, "No further changes are allowed after the builder has built the column.");
    data.position(data.capacity());
    return this;
  }

  @Override
  public DoubleColumnBuilder expand(final int size) throws BufferOverflowException {
    Preconditions.checkArgument(
        !built, "No further changes are allowed after the builder has built the column.");
    Preconditions.checkArgument(size >= 0);
    data.position(data.position() + size);
    return this;
  }

  @Override
  public DoubleColumn build() {
    built = true;
    return new DoubleColumn(data.array(), data.position());
  }

  @Override
  public DoubleMutableColumn buildMutable() {
    built = true;
    return new DoubleMutableColumn(data.array(), data.position());
  }

  @Override
  public void replaceDouble(final double value, final int row) throws IndexOutOfBoundsException {
    Preconditions.checkArgument(
        !built, "No further changes are allowed after the builder has built the column.");
    Preconditions.checkElementIndex(row, data.position());
    data.put(row, value);
  }

  @Override
  @Deprecated
  public Double getObject(final int row) {
    return data.get(row);
  }

  @Override
  public double getDouble(final int row) {
    return data.get(row);
  }

  @Override
  public DoubleColumnBuilder forkNewBuilder() {
    double[] arr = new double[data.array().length];
    System.arraycopy(data.array(), 0, arr, 0, data.position());
    return new DoubleColumnBuilder(
        (DoubleBuffer) DoubleBuffer.wrap(arr).position(data.position()).limit(data.limit()));
  }
}
