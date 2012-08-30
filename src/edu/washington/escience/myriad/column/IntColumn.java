package edu.washington.escience.myriad.column;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.proto.TransportProto.ColumnMessage;
import edu.washington.escience.myriad.proto.TransportProto.ColumnMessage.ColumnMessageType;
import edu.washington.escience.myriad.proto.TransportProto.IntColumnMessage;

/**
 * A column of Integer values.
 * 
 * @author dhalperi
 * 
 */
public final class IntColumn implements Column {
  /** Internal representation of the column data. */
  private final ByteBuffer dataBytes;
  /** View of the column data as ints. */
  private final IntBuffer data;

  /** Constructs an empty column that can hold up to TupleBatch.BATCH_SIZE elements. */
  public IntColumn() {
    this.dataBytes = ByteBuffer.allocate(TupleBatch.BATCH_SIZE * (Integer.SIZE / Byte.SIZE));
    this.data = dataBytes.asIntBuffer();
  }

  /**
   * Constructs an IntColumn by deserializing the given ColumnMessage.
   * 
   * @param message a ColumnMessage containing the contents of this column.
   */
  public IntColumn(final ColumnMessage message) {
    if (message.getType().ordinal() != ColumnMessageType.INT_VALUE) {
      throw new IllegalArgumentException("Trying to construct IntColumn from non-INT ColumnMessage");
    }
    if (!message.hasIntColumn()) {
      throw new IllegalArgumentException("ColumnMessage has type INT but no IntColumn");
    }
    this.dataBytes = message.getIntColumn().getData().asReadOnlyByteBuffer();
    this.data = dataBytes.asIntBuffer();
    this.data.position(message.getNumTuples());
  }

  @Override
  public Object get(final int row) {
    Preconditions.checkElementIndex(row, data.position());
    return Integer.valueOf(data.get(row));
  }

  /**
   * Returns the element at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   */
  public int getInt(final int row) {
    return data.get(row);
  }

  @Override
  public void getIntoJdbc(final int row, final PreparedStatement statement, final int jdbcIndex)
      throws SQLException {
    statement.setInt(jdbcIndex, getInt(row));
  }

  @Override
  public void getIntoSQLite(final int row, final SQLiteStatement statement, final int sqliteIndex)
      throws SQLiteException {
    statement.bind(sqliteIndex, getInt(row));
  }

  @Override
  public Column putObject(final Object value) {
    return put((Integer) value);
  }

  @Override
  public Column putFromJdbc(final ResultSet resultSet, final int jdbcIndex) throws SQLException {
    return put(resultSet.getInt(jdbcIndex));
  }

  @Override
  public void putFromSQLite(final SQLiteStatement statement, final int index)
      throws SQLiteException {
    throw new UnsupportedOperationException("SQLite does not support Int columns.");
  }

  /**
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   * @return this column.
   */
  public IntColumn put(final int value) {
    data.put(value);
    return this;
  }

  @Override
  public ColumnMessage serializeToProto() {
    /* Note that we do *not* build the inner class. We pass its builder instead. */
    IntColumnMessage.Builder inner =
        IntColumnMessage.newBuilder().setData(ByteString.copyFrom(dataBytes));
    return ColumnMessage.newBuilder().setType(ColumnMessageType.INT).setNumTuples(size())
        .setIntColumn(inner).build();
  }

  @Override
  public int size() {
    return data.position();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
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