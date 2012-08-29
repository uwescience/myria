package edu.washington.escience.myriad.column;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.proto.TransportProto.ColumnMessage;
import edu.washington.escience.myriad.proto.TransportProto.ColumnMessage.ColumnMessageType;
import edu.washington.escience.myriad.proto.TransportProto.StringColumnMessage;

/**
 * A column of String values.
 * 
 * @author dhalperi
 * 
 */
public final class StringColumn implements Column {
  /**
   * The positions of the starts of each String in this column. Used to pack variable-length Strings
   * and yet still have fast lookup.
   */
  private final IntBuffer startIndices;
  /** Internal structure for startIndices. */
  private final ByteBuffer startIndicesBytes;
  /**
   * The positions of the ends of each String in this column. Used to pack variable-length Strings
   * and yet still have fast lookup.
   */
  private final IntBuffer endIndices;
  /** Internal structure for endIndices. */
  private final ByteBuffer endIndicesBytes;
  /** Contains the packed character data. */
  private final StringBuilder data;
  /** Number of elements in this column. */
  private int numStrings;

  /** Constructs an empty column that can hold up to TupleBatch.BATCH_SIZE elements. */
  public StringColumn() {
    this.startIndicesBytes =
        ByteBuffer.allocate(TupleBatch.BATCH_SIZE * (Integer.SIZE / Byte.SIZE));
    this.startIndices = startIndicesBytes.asIntBuffer();
    this.endIndicesBytes = ByteBuffer.allocate(TupleBatch.BATCH_SIZE * (Integer.SIZE / Byte.SIZE));
    this.endIndices = endIndicesBytes.asIntBuffer();
    this.data = new StringBuilder();
    this.numStrings = 0;
  }

  /**
   * Constructs an empty column that can hold up to TupleBatch.BATCH_SIZE elements, but uses the
   * averageStringSize to seed the initial size of the internal buffer that stores the
   * variable-length Strings.
   * 
   * @param averageStringSize expected average size of the Strings that will be stored in this
   *          column.
   */
  public StringColumn(final int averageStringSize) {
    this.startIndicesBytes =
        ByteBuffer.allocate(TupleBatch.BATCH_SIZE * (Integer.SIZE / Byte.SIZE));
    this.startIndices = startIndicesBytes.asIntBuffer();
    this.endIndicesBytes = ByteBuffer.allocate(TupleBatch.BATCH_SIZE * (Integer.SIZE / Byte.SIZE));
    this.endIndices = endIndicesBytes.asIntBuffer();
    this.data = new StringBuilder(averageStringSize * TupleBatch.BATCH_SIZE);
    this.numStrings = 0;
  }

  @Override
  public Object get(final int row) {
    return getString(row);
  }

  @Override
  public void getIntoJdbc(final int row, final PreparedStatement statement, final int jdbcIndex)
      throws SQLException {
    statement.setString(jdbcIndex, getString(row));
  }

  @Override
  public void getIntoSQLite(final int row, final SQLiteStatement statement, final int sqliteIndex)
      throws SQLiteException {
    statement.bind(sqliteIndex, getString(row));
  }

  /**
   * Returns the element at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   */
  public String getString(final int row) {
    Preconditions.checkElementIndex(row, numStrings);
    return data.substring(startIndices.get(row), endIndices.get(row));
  }

  @Override
  public void put(final Object value) {
    putString((String) value);
  }

  @Override
  public void putFromJdbc(final ResultSet resultSet, final int jdbcIndex) throws SQLException {
    putString(resultSet.getString(jdbcIndex));
  }

  @Override
  public void putFromSQLite(final SQLiteStatement statement, final int index)
      throws SQLiteException {
    putString(statement.columnString(index));
  }

  /**
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   */
  public void putString(final String value) {
    startIndices.put(data.length());
    data.append(value);
    endIndices.put(data.length());
    numStrings++;
  }

  @Override
  public Message serializeToProto() throws IOException {
    /* Note that we do *not* build the inner class. We pass its builder instead. */
    StringColumnMessage.Builder inner =
        StringColumnMessage.newBuilder().setData(ByteString.copyFromUtf8(data.toString()));
    inner.setStartIndices(ByteString.copyFrom(startIndicesBytes));
    inner.setEndIndices(ByteString.copyFrom(endIndicesBytes));
    return ColumnMessage.newBuilder().setType(ColumnMessageType.STRING).setNumTuples(size())
        .setStringColumn(inner).build();
  }

  @Override
  public int size() {
    return numStrings;
  }
}