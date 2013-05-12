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
import edu.washington.escience.myriad.proto.DataProto.DoubleColumnMessage;

/**
 * A column of Double values.
 * 
 * @author dhalperi
 * 
 */
public final class DoubleColumn implements Column<Double> {
  /** Internal representation of the column data. */
  private final double[] data;
  /** The number of existing rows in this column. */
  private int position;

  /** Constructs an empty column that can hold up to TupleBatch.BATCH_SIZE elements. */
  public DoubleColumn() {
    data = new double[TupleBatch.BATCH_SIZE];
    position = 0;
  }

  /**
   * Constructs a DoubleColumn by deserializing the given ColumnMessage.
   * 
   * @param message a ColumnMessage containing the contents of this column.
   * @param numTuples num tuples in the column message
   */
  public DoubleColumn(final ColumnMessage message, final int numTuples) {
    if (message.getType().ordinal() != ColumnMessage.Type.DOUBLE_VALUE) {
      throw new IllegalArgumentException("Trying to construct DoubleColumn from non-DOUBLE ColumnMessage");
    }
    if (!message.hasDoubleColumn()) {
      throw new IllegalArgumentException("ColumnMessage has type DOUBLE but no DoubleColumn");
    }
    data = new double[TupleBatch.BATCH_SIZE];
    position = 0;
    try {
      byte[] dataBytes = message.getDoubleColumn().getData().toByteArray();
      ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(dataBytes));
      for (int i = 0; i < numTuples; ++i) {
        data[i] = input.readDouble();
      }
      input.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    position = numTuples;
  }

  @Override
  public Double get(final int row) {
    return Double.valueOf(getDouble(row));
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
   * Returns the element at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   */
  public double getDouble(final int row) {
    Preconditions.checkElementIndex(row, position);
    return data[row];
  }

  @Override
  public Type getType() {
    return Type.DOUBLE_TYPE;
  }

  /**
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   * @return this column.
   */
  public DoubleColumn put(final double value) {
    Preconditions.checkElementIndex(position, TupleBatch.BATCH_SIZE);
    data[position++] = value;
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
    ByteArrayOutputStream byteArray = new ByteArrayOutputStream(position * (Double.SIZE / Byte.SIZE));
    try {
      ObjectOutputStream output = new ObjectOutputStream(byteArray);
      for (int i = 0; i < position; ++i) {
        output.writeDouble(data[i]);
      }
      output.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    final DoubleColumnMessage.Builder inner =
        DoubleColumnMessage.newBuilder().setData(ByteString.copyFrom(byteArray.toByteArray()));
    /* Note that we do *not* build the inner class. We pass its builder instead. */
    return ColumnMessage.newBuilder().setType(ColumnMessage.Type.DOUBLE).setDoubleColumn(inner).build();
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