package edu.washington.escience.myriad.column;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.protobuf.ByteString;

import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage.ColumnMessageType;
import edu.washington.escience.myriad.proto.DataProto.LongColumnMessage;

/**
 * A column of Long values.
 * 
 * @author dhalperi
 * 
 */
public final class LongColumn implements Column<Long> {
  /** Internal representation of the column data. */
  private final ByteBuffer dataBytes;
  /** View of the column data as longs. */
  private final LongBuffer data;

  /** Constructs an empty column that can hold up to TupleBatch.BATCH_SIZE elements. */
  public LongColumn() {
    dataBytes = ByteBuffer.allocate(TupleBatch.BATCH_SIZE * (Long.SIZE / Byte.SIZE));
    data = dataBytes.asLongBuffer();
  }

  /**
   * Constructs a LongColumn by deserializing the given ColumnMessage.
   * 
   * @param message a ColumnMessage containing the contents of this column.
   * @param numTuples num tuples in the column message
   */
  public LongColumn(final ColumnMessage message, final int numTuples) {
    if (message.getType().ordinal() != ColumnMessageType.LONG_VALUE) {
      throw new IllegalArgumentException("Trying to construct LongColumn from non-LONG ColumnMessage");
    }
    if (!message.hasLongColumn()) {
      throw new IllegalArgumentException("ColumnMessage has type LONG but no LongColumn");
    }
    dataBytes = message.getLongColumn().getData().asReadOnlyByteBuffer();
    data = dataBytes.asLongBuffer();
    data.position(numTuples);
  }

  @Override
  public Long get(final int row) {
    return Long.valueOf(getLong(row));
  }

  @Override
  public void getIntoJdbc(final int row, final PreparedStatement statement, final int jdbcIndex) throws SQLException {
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

  /**
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   * @return this column.
   */
  public LongColumn put(final long value) {
    data.put(value);
    return this;
  }

  @Override
  public Column<Long> putFromJdbc(final ResultSet resultSet, final int jdbcIndex) throws SQLException {
    return put(resultSet.getLong(jdbcIndex));
  }

  @Override
  public void putFromSQLite(final SQLiteStatement statement, final int index) throws SQLiteException {
    put(statement.columnLong(index));
  }

  @Override
  public Column<Long> putObject(final Object value) {
    return put((Long) value);
  }

  @Override
  public ColumnMessage serializeToProto() {
    /* Note that we do *not* build the inner class. We pass its builder instead. */
    final LongColumnMessage.Builder inner = LongColumnMessage.newBuilder().setData(ByteString.copyFrom(dataBytes));
    return ColumnMessage.newBuilder().setType(ColumnMessageType.LONG).setLongColumn(inner).build();
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