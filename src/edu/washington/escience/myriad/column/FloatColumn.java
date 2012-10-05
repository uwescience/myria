package edu.washington.escience.myriad.column;

import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage.ColumnMessageType;
import edu.washington.escience.myriad.proto.DataProto.FloatColumnMessage;
// import edu.washington.escience.myriad.proto.TransportProto.ColumnMessage;
// import edu.washington.escience.myriad.proto.TransportProto.ColumnMessage.ColumnMessageType;
// import edu.washington.escience.myriad.proto.TransportProto.FloatColumnMessage;

/**
 * A column of Float values.
 * 
 * @author dhalperi
 * 
 */
public final class FloatColumn implements Column {
  /** Internal representation of the column data. */
  private final ByteBuffer dataBytes;
  /** View of the column data as floats. */
  private final FloatBuffer data;

  /** Constructs an empty column that can hold up to TupleBatch.BATCH_SIZE elements. */
  public FloatColumn() {
    this.dataBytes = ByteBuffer.allocate(TupleBatch.BATCH_SIZE * (Float.SIZE / Byte.SIZE));
    this.data = dataBytes.asFloatBuffer();
  }

  /**
   * Constructs a FloatColumn by deserializing the given ColumnMessage.
   * 
   * @param message a ColumnMessage containing the contents of this column.
   */
  public FloatColumn(final ColumnMessage message) {
    if (message.getType().ordinal() != ColumnMessageType.FLOAT_VALUE) {
      throw new IllegalArgumentException("Trying to construct FloatColumn from non-FLOAT ColumnMessage");
    }
    if (!message.hasFloatColumn()) {
      throw new IllegalArgumentException("ColumnMessage has type FLOAT but no FloatColumn");
    }
    this.dataBytes = message.getFloatColumn().getData().asReadOnlyByteBuffer();
    this.data = dataBytes.asFloatBuffer();
    this.data.position(message.getNumTuples());
  }

  @Override
  public Object get(final int row) {
    return Float.valueOf(getFloat(row));
  }

  /**
   * Returns the element at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   */
  public float getFloat(final int row) {
    Preconditions.checkElementIndex(row, data.position());
    return data.get(row);
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
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   * @return this column.
   */
  public FloatColumn put(final float value) {
    Preconditions.checkElementIndex(data.position(), data.capacity());
    data.put(value);
    return this;
  }

  @Override
  public Column putFromJdbc(final ResultSet resultSet, final int jdbcIndex) throws SQLException {
    return put(resultSet.getFloat(jdbcIndex));
  }

  @Override
  public void putFromSQLite(final SQLiteStatement statement, final int index) throws SQLiteException {
    throw new UnsupportedOperationException("SQLite does not support Float columns.");
  }

  @Override
  public Column putObject(final Object value) {
    return put((Float) value);
  }

  @Override
  public ColumnMessage serializeToProto() {
    /* Note that we do *not* build the inner class. We pass its builder instead. */
    final FloatColumnMessage.Builder inner = FloatColumnMessage.newBuilder().setData(ByteString.copyFrom(dataBytes));
    return ColumnMessage.newBuilder().setType(ColumnMessageType.FLOAT).setNumTuples(size()).setFloatColumn(inner)
        .build();
  }

  @Override
  public int size() {
    return data.position();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(size()).append(" elements: [");
    for (int i = 0; i < size(); ++i) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(data.get(i));
    }
    sb.append("]");
    return sb.toString();
  }
}