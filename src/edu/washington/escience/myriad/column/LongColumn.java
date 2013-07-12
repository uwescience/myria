package edu.washington.escience.myriad.column;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hasher;
import com.google.protobuf.ByteString;

import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;
import edu.washington.escience.myriad.proto.DataProto.LongColumnMessage;

/**
 * A column of Long values.
 * 
 * @author dhalperi
 * 
 */
public final class LongColumn implements Column<Long> {
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  /** Internal representation of the column data. */
  private final long[] data;
  /** The number of existing rows in this column. */
  private final int position;

  /**
   * Constructs a new column.
   * 
   * @param data the data
   * @param numData number of tuples.
   * */
  public LongColumn(final long[] data, final int numData) {
    this.data = data;
    position = numData;
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
    Preconditions.checkElementIndex(row, position);
    return data[row];
  }

  @Override
  public Type getType() {
    return Type.LONG_TYPE;
  }

  @Override
  public ColumnMessage serializeToProto() {
    ByteBuffer dataBytes = ByteBuffer.allocate(position * Long.SIZE / Byte.SIZE);
    for (int i = 0; i < position; i++) {
      dataBytes.putLong(data[i]);
    }

    dataBytes.flip();
    final LongColumnMessage.Builder inner = LongColumnMessage.newBuilder().setData(ByteString.copyFrom(dataBytes));

    return ColumnMessage.newBuilder().setType(ColumnMessage.Type.LONG).setLongColumn(inner).build();
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
    return getLong(leftIdx) == ((LongColumn) rightColumn).getLong(rightIdx);
  }

  @Override
  public void append(final int index, final ColumnBuilder<?> columnBuilder) {
    ((LongColumnBuilder) columnBuilder).append(getLong(index));
  }

  @Override
  public void addToHasher(final int row, final Hasher hasher) {
    hasher.putLong(getLong(row));
  }
}