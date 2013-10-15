package edu.washington.escience.myria.column;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.mutable.FloatMutableColumn;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;

/**
 * A column of Float values.
 * 
 */
public final class FloatColumnBuilder implements ColumnBuilder<Float> {
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
      throw new IllegalArgumentException("Trying to construct FloatColumn from non-FLOAT ColumnMessage");
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

  /**
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   * @return this column.
   * @throws BufferOverflowException if exceeds buffer up bound.
   */
  public FloatColumnBuilder append(final float value) throws BufferOverflowException {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    data.put(value);
    return this;
  }

  @Override
  public FloatColumnBuilder appendObject(final Object value) throws BufferOverflowException {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    return append((Float) value);
  }

  @Override
  public FloatColumnBuilder appendFromJdbc(final ResultSet resultSet, final int jdbcIndex) throws SQLException,
      BufferOverflowException {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    return append(resultSet.getFloat(jdbcIndex));
  }

  @Override
  public FloatColumnBuilder appendFromSQLite(final SQLiteStatement statement, final int index) throws SQLiteException,
      BufferOverflowException {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    return append((float) statement.columnDouble(index));
  }

  @Override
  public FloatColumnBuilder append(final Float value) throws BufferOverflowException {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    return append(value.floatValue());
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
  @Deprecated
  public FloatColumnBuilder replace(final int idx, final Float value) throws IndexOutOfBoundsException {
    return replace(idx, value.floatValue());
  }

  /**
   * Replace the specified element.
   * 
   * @param value element to be inserted.
   * @param idx where to insert the element.
   * @return this column builder.
   * @throws IndexOutOfBoundsException if the idx exceeds the currently valid indices, i.e. the currently built size.
   */
  public FloatColumnBuilder replace(final int idx, final float value) throws IndexOutOfBoundsException {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    Preconditions.checkElementIndex(idx, data.position());
    data.put(idx, value);
    return this;
  }

  @Override
  public FloatColumnBuilder expand(final int size) {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    Preconditions.checkArgument(size >= 0);
    data.position(data.position() + size);
    return this;
  }

  @Override
  public FloatColumnBuilder expandAll() {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    data.position(data.capacity());
    return this;
  }

  @Override
  @Deprecated
  public Float get(final int row) {
    return data.get(row);
  }

  /**
   * @param row the row to get
   * @return primitive value of the row
   * */
  public float getFloat(final int row) {
    return data.get(row);
  }

  @Override
  public FloatColumnBuilder forkNewBuilder() {
    float[] arr = new float[data.array().length];
    System.arraycopy(data.array(), 0, arr, 0, data.position());
    return new FloatColumnBuilder((FloatBuffer) FloatBuffer.wrap(arr).position(data.position()).limit(data.limit()));
  }
}