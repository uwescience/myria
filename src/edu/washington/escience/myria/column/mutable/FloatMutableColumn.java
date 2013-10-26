package edu.washington.escience.myria.column.mutable;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hasher;
import com.google.protobuf.ByteString;

import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.FloatColumn;
import edu.washington.escience.myria.column.builder.ColumnBuilder;
import edu.washington.escience.myria.column.builder.FloatColumnBuilder;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.proto.DataProto.FloatColumnMessage;

/**
 * A mutable column of Float values.
 * 
 */
public final class FloatMutableColumn implements MutableColumn<Float> {
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  /** Internal representation of the column data. */
  private final float[] data;
  /** The number of existing rows in this column. */
  private final int position;

  /**
   * Constructs a new column.
   * 
   * @param data the underlying data
   * @param numData number of tuples.
   * */
  public FloatMutableColumn(final float[] data, final int numData) {
    Preconditions.checkNotNull(data);
    Preconditions.checkArgument(numData <= TupleBatch.BATCH_SIZE);
    this.data = data;
    position = numData;
  }

  @Override
  public Float get(final int row) {
    return Float.valueOf(getFloat(row));
  }

  @Override
  public void getIntoJdbc(final int row, final PreparedStatement statement, final int jdbcIndex) throws SQLException {
    statement.setFloat(jdbcIndex, getFloat(row));
  }

  @Override
  public void getIntoSQLite(final int row, final SQLiteStatement statement, final int sqliteIndex)
      throws SQLiteException {
    statement.bind(sqliteIndex, getFloat(row));
  }

  /**
   * Returns the element at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   */
  public float getFloat(final int row) {
    Preconditions.checkElementIndex(row, position);
    return data[row];
  }

  @Override
  public Type getType() {
    return Type.FLOAT_TYPE;
  }

  @Override
  public ColumnMessage serializeToProto() {
    ByteBuffer dataBytes = ByteBuffer.allocate(position * Float.SIZE / Byte.SIZE);
    for (int i = 0; i < position; i++) {
      dataBytes.putFloat(data[i]);
    }
    dataBytes.flip();
    final FloatColumnMessage.Builder inner = FloatColumnMessage.newBuilder().setData(ByteString.copyFrom(dataBytes));

    return ColumnMessage.newBuilder().setType(ColumnMessage.Type.FLOAT).setFloatColumn(inner).build();
  }

  @Override
  public int size() {
    return position;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(size()).append(" elements: [");
    for (int i = 0; i < size(); ++i) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(data[i]);
    }
    sb.append(']');
    return sb.toString();
  }

  @Override
  public boolean equals(final int leftIdx, final Column<?> rightColumn, final int rightIdx) {
    return getFloat(leftIdx) == ((FloatMutableColumn) rightColumn).getFloat(rightIdx);
  }

  @Override
  public void append(final int index, final ColumnBuilder<?> columnBuilder) {
    ((FloatColumnBuilder) columnBuilder).append(getFloat(index));
  }

  @Override
  public void addToHasher(final int row, final Hasher hasher) {
    hasher.putFloat(getFloat(row));
  }

  @Override
  public void replace(final int index, final Float value) {
    replace(index, value);
  }

  /**
   * replace the value on a row with the given float value.
   * 
   * @param index row index
   * @param value the float value.
   */
  public void replace(final int index, final float value) {
    Preconditions.checkElementIndex(index, size());
    data[index] = value;
  }

  @Override
  public FloatColumn toColumn() {
    return new FloatColumn(data.clone(), position);
  }

  @Override
  public FloatMutableColumn cloneItself() {
    return new FloatMutableColumn(data.clone(), position);
  }
}