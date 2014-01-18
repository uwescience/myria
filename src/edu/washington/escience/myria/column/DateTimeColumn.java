package edu.washington.escience.myria.column;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.joda.time.DateTime;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hasher;
import com.google.protobuf.ByteString;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.builder.ColumnBuilder;
import edu.washington.escience.myria.column.builder.DateTimeColumnBuilder;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.proto.DataProto.DateTimeColumnMessage;
import edu.washington.escience.myria.util.ImmutableIntArray;
import edu.washington.escience.myria.util.TypeFunnel;

/**
 * A column of Date values.
 * 
 */
public final class DateTimeColumn extends Column<DateTime> {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1;
  /** Internal representation of the column data. */
  private final DateTime[] data;
  /** The number of existing rows in this column. */
  private final int position;

  /**
   * Constructs a new column.
   * 
   * @param data the data
   * @param numData number of tuples.
   * */
  public DateTimeColumn(final DateTime[] data, final int numData) {
    this.data = data;
    position = numData;
  }

  @Override
  public DateTime getObject(final int row) {
    return getDateTime(row);
  }

  @Override
  public void getIntoJdbc(final int row, final PreparedStatement statement, final int jdbcIndex) throws SQLException {
    statement.setTimestamp(jdbcIndex, new Timestamp(getDateTime(row).getMillis()));
  }

  @Override
  public void getIntoSQLite(final int row, final SQLiteStatement statement, final int sqliteIndex)
      throws SQLiteException {
    statement.bind(sqliteIndex, getDateTime(row).getMillis()); // SQLite long
  }

  /**
   * Returns the element at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   */
  @Override
  public DateTime getDateTime(final int row) {
    Preconditions.checkElementIndex(row, position);
    return data[row];
  }

  @Override
  public Type getType() {
    return Type.DATETIME_TYPE;
  }

  @Override
  public ColumnMessage serializeToProto() {
    ByteBuffer dataBytes = ByteBuffer.allocate(position * Long.SIZE / Byte.SIZE);
    for (int i = 0; i < position; i++) {
      dataBytes.putLong(data[i].getMillis());
    }

    dataBytes.flip();
    final DateTimeColumnMessage.Builder inner =
        DateTimeColumnMessage.newBuilder().setData(ByteString.copyFrom(dataBytes));

    return ColumnMessage.newBuilder().setType(ColumnMessage.Type.DATETIME).setDateColumn(inner).build();
  }

  @Override
  public ColumnMessage serializeToProto(final ImmutableIntArray validIndices) {
    ByteBuffer dataBytes = ByteBuffer.allocate(validIndices.length() * Long.SIZE / Byte.SIZE);
    for (int i : validIndices) {
      dataBytes.putLong(data[i].getMillis());
    }

    dataBytes.flip();
    final DateTimeColumnMessage.Builder inner =
        DateTimeColumnMessage.newBuilder().setData(ByteString.copyFrom(dataBytes));

    return ColumnMessage.newBuilder().setType(ColumnMessage.Type.DATETIME).setDateColumn(inner).build();
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
    return getDateTime(leftIdx).equals(rightColumn.getObject(rightIdx));
  }

  @Override
  public void append(final int index, final ColumnBuilder<?> columnBuilder) {
    ((DateTimeColumnBuilder) columnBuilder).appendDateTime(getDateTime(index));
  }

  @Override
  public void addToHasher(final int row, final Hasher hasher) {
    hasher.putObject(getDateTime(row), TypeFunnel.INSTANCE);
  }
}