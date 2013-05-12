package edu.washington.escience.myriad.column;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;
import edu.washington.escience.myriad.proto.DataProto.IntColumnMessage;

/**
 * A column of Int values.
 * 
 * @author dhalperi
 * 
 */
public final class IntColumn implements Column<Integer> {
  /** Internal representation of the column data. */
  private final int[] data;
  /** The number of existing rows in this column. */
  private int position;

  /** Constructs an empty column that can hold up to TupleBatch.BATCH_SIZE elements. */
  public IntColumn() {
    data = new int[TupleBatch.BATCH_SIZE];
    position = 0;
  }

  /**
   * Constructs a IntColumn by deserializing the given ColumnMessage.
   * 
   * @param message a ColumnMessage containing the contents of this column.
   * @param numTuples num tuples in the column message
   */
  public IntColumn(final ColumnMessage message, final int numTuples) {
    if (message.getType().ordinal() != ColumnMessage.Type.INT_VALUE) {
      throw new IllegalArgumentException("Trying to construct IntColumn from non-INT ColumnMessage");
    }
    if (!message.hasIntColumn()) {
      throw new IllegalArgumentException("ColumnMessage has type INT but no IntColumn");
    }
    data = new int[TupleBatch.BATCH_SIZE];
    position = 0;
    try {
      byte[] dataBytes = message.getIntColumn().getData().toByteArray();
      ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(dataBytes));
      for (int i = 0; i < numTuples; ++i) {
        data[i] = input.readInt();
      }
      input.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    position = numTuples;
  }

  @Override
  public Integer get(final int row) {
    return Integer.valueOf(getInt(row));
  }

  @Override
  public void getIntoJdbc(final int row, final PreparedStatement statement, final int jdbcIndex) throws SQLException {
    statement.setInt(jdbcIndex, getInt(row));
  }

  @Override
  public void getIntoSQLite(final int row, final SQLiteStatement statement, final int sqliteIndex)
      throws SQLiteException {
    statement.bind(sqliteIndex, getInt(row));
  }

  /**
   * Returns the element at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   */
  public int getInt(final int row) {
    Preconditions.checkElementIndex(row, position);
    return data[row];
  }

  @Override
  public Type getType() {
    return Type.INT_TYPE;
  }

  /**
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   * @return this column.
   */
  public IntColumn put(final int value) {
    Preconditions.checkElementIndex(position, TupleBatch.BATCH_SIZE);
    data[position++] = value;
    return this;
  }

  @Override
  public Column<Integer> putFromJdbc(final ResultSet resultSet, final int jdbcIndex) throws SQLException {
    return put(resultSet.getInt(jdbcIndex));
  }

  @Override
  public void putFromSQLite(final SQLiteStatement statement, final int index) throws SQLiteException {
    put(statement.columnInt(index));
  }

  @Override
  public Column<Integer> putObject(final Object value) {
    return put((Integer) value);
  }

  @Override
  public ColumnMessage serializeToProto() {
    ByteArrayOutputStream byteArray = new ByteArrayOutputStream(position * (Integer.SIZE / Byte.SIZE));
    try {
      ObjectOutputStream output = new ObjectOutputStream(byteArray);
      for (int i = 0; i < position; ++i) {
        output.writeInt(data[i]);
      }
      output.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    final IntColumnMessage.Builder inner =
        IntColumnMessage.newBuilder().setData(ByteString.copyFrom(byteArray.toByteArray()));
    /* Note that we do *not* build the inner class. We pass its builder instead. */
    return ColumnMessage.newBuilder().setType(ColumnMessage.Type.INT).setIntColumn(inner).build();
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
}