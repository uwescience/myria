package edu.washington.escience.myriad.column;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;

import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.proto.TransportProto.ColumnMessage;
import edu.washington.escience.myriad.proto.TransportProto.ColumnMessage.ColumnMessageType;
import edu.washington.escience.myriad.proto.TransportProto.DoubleColumnMessage;

/**
 * A column of Double values.
 * 
 * @author dhalperi
 * 
 */
public final class DoubleColumn implements Column {
  /** Internal representation of the column data. */
  private final ByteBuffer dataBytes;
  /** View of the column data as doubles. */
  private final DoubleBuffer data;

  /** Constructs an empty column that can hold up to TupleBatch.BATCH_SIZE elements. */
  public DoubleColumn() {
    this.dataBytes = ByteBuffer.allocate(TupleBatch.BATCH_SIZE * (Double.SIZE / Byte.SIZE));
    this.data = dataBytes.asDoubleBuffer();
  }

  @Override
  public Object get(final int row) {
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
  public void getIntoJdbc(final int row, final PreparedStatement statement, final int jdbcIndex)
      throws SQLException {
    statement.setDouble(jdbcIndex, getDouble(row));
  }

  @Override
  public void getIntoSQLite(final int row, final SQLiteStatement statement, final int sqliteIndex)
      throws SQLiteException {
    statement.bind(sqliteIndex, getDouble(row));
  }

  @Override
  public void put(final Object value) {
    putDouble((Double) value);
  }

  /**
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   */
  public void putDouble(final double value) {
    Preconditions.checkElementIndex(data.position(), data.capacity());
    data.put(value);
  }

  @Override
  public void putFromJdbc(final ResultSet resultSet, final int jdbcIndex) throws SQLException {
    putDouble(resultSet.getDouble(jdbcIndex));
  }

  @Override
  public void putFromSQLite(final SQLiteStatement statement, final int index)
      throws SQLiteException {
    putDouble(statement.columnDouble(index));
  }

  @Override
  public void serializeToProto(final CodedOutputStream output) throws IOException {
    /* Note that we do *not* build the inner class. We pass its builder instead. */
    DoubleColumnMessage.Builder inner =
        DoubleColumnMessage.newBuilder().setData(ByteString.copyFrom(dataBytes));
    ColumnMessage.newBuilder().setType(ColumnMessageType.DOUBLE).setNumTuples(size())
        .setDoubleColumn(inner).build().writeTo(output);
  }

  @Override
  public int size() {
    return data.position();
  }
}