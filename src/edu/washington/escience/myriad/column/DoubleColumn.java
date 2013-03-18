package edu.washington.escience.myriad.column;

import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
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
import edu.washington.escience.myriad.proto.DataProto.DoubleColumnMessage;

/**
 * A column of Double values.
 * 
 * @author dhalperi
 * 
 */
public final class DoubleColumn implements Column<Double> {
  /** Internal representation of the column data. */
  private final ByteBuffer dataBytes;
  /** View of the column data as doubles. */
  private final DoubleBuffer data;

  /** Constructs an empty column that can hold up to TupleBatch.BATCH_SIZE elements. */
  public DoubleColumn() {
    dataBytes = ByteBuffer.allocate(TupleBatch.BATCH_SIZE * (Double.SIZE / Byte.SIZE));
    data = dataBytes.asDoubleBuffer();
  }

  /**
   * Constructs a DoubleColumn by deserializing the given ColumnMessage.
   * 
   * @param message a ColumnMessage containing the contents of this column.
   * @param numTuples num tuples in the column message
   */
  public DoubleColumn(final ColumnMessage message, final int numTuples) {
    if (message.getType().ordinal() != ColumnMessageType.DOUBLE_VALUE) {
      throw new IllegalArgumentException("Trying to construct DoubleColumn from non-DOUBLE ColumnMessage");
    }
    if (!message.hasDoubleColumn()) {
      throw new IllegalArgumentException("ColumnMessage has type DOUBLE but no DoubleColumn");
    }
    dataBytes = message.getDoubleColumn().getData().asReadOnlyByteBuffer();
    data = dataBytes.asDoubleBuffer();
    data.position(numTuples);
  }

  @Override
  public Double get(final int row) {
    return Double.valueOf(getDouble(row));
  }

  /**
   * Returns the element at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   */
  public double getDouble(final int row) {
    Preconditions.checkElementIndex(row, data.position());
    return data.get(row);
  }

  @Override
  public void getIntoJdbc(final int row, final PreparedStatement statement, final int jdbcIndex) throws SQLException {
    statement.setDouble(jdbcIndex, getDouble(row));
  }

  @Override
  public void getIntoSQLite(final int row, final SQLiteStatement statement, final int sqliteIndex)
      throws SQLiteException {
    statement.bind(sqliteIndex, getDouble(row));
  }

  /**
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   * @return this column.
   */
  public DoubleColumn put(final double value) {
    Preconditions.checkElementIndex(data.position(), data.capacity());
    data.put(value);
    return this;
  }

  @Override
  public Column<Double> putFromJdbc(final ResultSet resultSet, final int jdbcIndex) throws SQLException {
    return put(resultSet.getDouble(jdbcIndex));
  }

  @Override
  public void putFromSQLite(final SQLiteStatement statement, final int index) throws SQLiteException {
    put(statement.columnDouble(index));
  }

  @Override
  public Column<Double> putObject(final Object value) {
    return put((Double) value);
  }

  @Override
  public ColumnMessage serializeToProto() {
    /* Note that we do *not* build the inner class. We pass its builder instead. */
    final DoubleColumnMessage.Builder inner = DoubleColumnMessage.newBuilder().setData(ByteString.copyFrom(dataBytes));
    return ColumnMessage.newBuilder().setType(ColumnMessageType.DOUBLE).setDoubleColumn(inner).build();
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
    sb.append(']');
    return sb.toString();
  }
}