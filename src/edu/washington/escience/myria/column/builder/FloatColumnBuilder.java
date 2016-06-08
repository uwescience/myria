package edu.washington.escience.myria.column.builder;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.FloatColumn;
import edu.washington.escience.myria.column.mutable.FloatMutableColumn;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * A column of Float values.
 *
 */
public final class FloatColumnBuilder extends ColumnBuilder<Float> {
  /** View of the column data as floats. */
  private final FloatBuffer data;

  /**
   * If the builder has built the column.
   * */
  private boolean built = false;

  /** Constructs an empty column that can hold up to TupleBatch.BATCH_SIZE elements. */
  public FloatColumnBuilder() {
    data = FloatBuffer.allocate(TupleBatch.BATCH_SIZE);
  }

  /**
   * copy.
   *
   * @param data the underlying data
   * */
  private FloatColumnBuilder(final FloatBuffer data) {
    this.data = data;
  }

  /**
   * Constructs a FloatColumn by deserializing the given ColumnMessage.
   *
   * @param message a ColumnMessage containing the contents of this column.
   * @param numTuples num tuples in the column message
   * @return the built column
   */
  public static FloatColumn buildFromProtobuf(final ColumnMessage message, final int numTuples) {
    if (message.getType().ordinal() != ColumnMessage.Type.FLOAT_VALUE) {
      throw new IllegalArgumentException(
          "Trying to construct FloatColumn from non-FLOAT ColumnMessage");
    }
    if (!message.hasFloatColumn()) {
      throw new IllegalArgumentException("ColumnMessage has type FLOAT but no FloatColumn");
    }
    ByteBuffer dataBytes = message.getFloatColumn().getData().asReadOnlyByteBuffer();
    FloatBuffer data = FloatBuffer.allocate(numTuples);
    for (int i = 0; i < numTuples; i++) {
      data.put(dataBytes.getFloat());
    }
    return new FloatColumn(data.array(), numTuples);
  }

  @Override
  public Type getType() {
    return Type.FLOAT_TYPE;
  }

  @Override
  public FloatColumnBuilder appendFloat(final float value) throws BufferOverflowException {
    Preconditions.checkArgument(
        !built, "No further changes are allowed after the builder has built the column.");
    data.put(value);
    return this;
  }

  @Deprecated
  @Override
  public FloatColumnBuilder appendObject(final Object value) throws BufferOverflowException {
    Preconditions.checkArgument(
        !built, "No further changes are allowed after the builder has built the column.");
    return appendFloat((Float) MyriaUtils.ensureObjectIsValidType(value));
  }

  @Override
  public FloatColumnBuilder appendFromJdbc(final ResultSet resultSet, final int jdbcIndex)
      throws SQLException, BufferOverflowException {
    Preconditions.checkArgument(
        !built, "No further changes are allowed after the builder has built the column.");
    return appendFloat(resultSet.getFloat(jdbcIndex));
  }

  @Override
  public FloatColumnBuilder appendFromSQLite(final SQLiteStatement statement, final int index)
      throws SQLiteException, BufferOverflowException {
    Preconditions.checkArgument(
        !built, "No further changes are allowed after the builder has built the column.");
    return appendFloat((float) statement.columnDouble(index));
  }

  @Override
  public int size() {
    return data.position();
  }

  @Override
  public FloatColumn build() {
    built = true;
    return new FloatColumn(data.array(), data.position());
  }

  @Override
  public FloatMutableColumn buildMutable() {
    built = true;
    return new FloatMutableColumn(data.array(), data.position());
  }

  @Override
  public void replaceFloat(final float value, final int row) throws IndexOutOfBoundsException {
    Preconditions.checkArgument(
        !built, "No further changes are allowed after the builder has built the column.");
    Preconditions.checkElementIndex(row, data.position());
    data.put(row, value);
  }

  @Override
  public FloatColumnBuilder expand(final int size) {
    Preconditions.checkArgument(
        !built, "No further changes are allowed after the builder has built the column.");
    Preconditions.checkArgument(size >= 0);
    data.position(data.position() + size);
    return this;
  }

  @Override
  public FloatColumnBuilder expandAll() {
    Preconditions.checkArgument(
        !built, "No further changes are allowed after the builder has built the column.");
    data.position(data.capacity());
    return this;
  }

  @Override
  @Deprecated
  public Float getObject(final int row) {
    return data.get(row);
  }

  @Override
  public float getFloat(final int row) {
    return data.get(row);
  }

  @Override
  public FloatColumnBuilder forkNewBuilder() {
    float[] arr = new float[data.array().length];
    System.arraycopy(data.array(), 0, arr, 0, data.position());
    return new FloatColumnBuilder(
        (FloatBuffer) FloatBuffer.wrap(arr).position(data.position()).limit(data.limit()));
  }
}
