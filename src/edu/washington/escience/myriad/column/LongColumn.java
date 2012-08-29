package edu.washington.escience.myriad.column;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.proto.TransportProto.ColumnMessage;
import edu.washington.escience.myriad.proto.TransportProto.ColumnMessage.ColumnMessageType;
import edu.washington.escience.myriad.proto.TransportProto.LongColumnMessage;

/**
 * A column of Long values.
 * 
 * @author dhalperi
 * 
 */
public final class LongColumn implements Column {
  /** Internal representation of the column data. */
  private final ByteBuffer dataBytes;
  /** View of the column data as longs. */
  private final LongBuffer data;

  /** Constructs an empty column that can hold up to TupleBatch.BATCH_SIZE elements. */
  public LongColumn() {
    this.dataBytes = ByteBuffer.allocate(TupleBatch.BATCH_SIZE * (Long.SIZE / Byte.SIZE));
    this.data = dataBytes.asLongBuffer();
  }

  @Override
  public Object get(final int row) {
    return Long.valueOf(getLong(row));
  }

  @Override
  public void getIntoJdbc(final int row, final PreparedStatement statement, final int jdbcIndex)
      throws SQLException {
    statement.setLong(jdbcIndex, getLong(row));
  }

  @Override
  public void getIntoSQLite(final int row, final SQLiteStatement statement, final int sqliteIndex)
      throws SQLiteException {
    statement.bind(sqliteIndex, getLong(row));
  }

  /**
   * Returns the element at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   */
  public long getLong(final int row) {
    return data.get(row);
  }

  @Override
  public void put(final Object value) {
    putLong((Long) value);
  }

  @Override
  public void putFromJdbc(final ResultSet resultSet, final int jdbcIndex) throws SQLException {
    putLong(resultSet.getLong(jdbcIndex));
  }

  @Override
  public void putFromSQLite(final SQLiteStatement statement, final int index)
      throws SQLiteException {
    putLong(statement.columnLong(index));
  }

  /**
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   */
  public void putLong(final long value) {
    data.put(value);
  }

  @Override
  public Message serializeToProto() throws IOException {
    /* Note that we do *not* build the inner class. We pass its builder instead. */
    LongColumnMessage.Builder inner =
        LongColumnMessage.newBuilder().setData(ByteString.copyFrom(dataBytes));
    return ColumnMessage.newBuilder().setType(ColumnMessageType.LONG).setNumTuples(size())
        .setLongColumn(inner).build();
  }

  @Override
  public int size() {
    return data.position();
  }
}