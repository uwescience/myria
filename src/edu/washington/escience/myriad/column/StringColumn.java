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
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage.ColumnMessageType;
import edu.washington.escience.myriad.proto.DataProto.StringColumnMessage;

/**
 * A column of String values.
 * 
 * @author dhalperi
 * 
 */
public final class StringColumn implements Column<String> {
  /**
   * The positions of the starts of each String in this column. Used to pack variable-length Strings and yet still have
   * fast lookup.
   */
  private final IntBuffer startIndices;
  /** Internal structure for startIndices. */
  private final ByteBuffer startIndicesBytes;
  /**
   * The positions of the ends of each String in this column. Used to pack variable-length Strings and yet still have
   * fast lookup.
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
    startIndicesBytes = ByteBuffer.allocate(TupleBatch.BATCH_SIZE * (Integer.SIZE / Byte.SIZE));
    startIndices = startIndicesBytes.asIntBuffer();
    endIndicesBytes = ByteBuffer.allocate(TupleBatch.BATCH_SIZE * (Integer.SIZE / Byte.SIZE));
    endIndices = endIndicesBytes.asIntBuffer();
    data = new StringBuilder();
    numStrings = 0;
  }

  /**
   * Constructs a StringColumn by deserializing the given ColumnMessage.
   * 
   * @param message a ColumnMessage containing the contents of this column.
   */
  public StringColumn(final ColumnMessage message) {
    if (message.getType().ordinal() != ColumnMessageType.STRING_VALUE) {
      throw new IllegalArgumentException("Trying to construct StringColumn from non-STRING ColumnMessage");
    }
    if (!message.hasStringColumn()) {
      throw new IllegalArgumentException("ColumnMessage has type STRING but no StringColumn");
    }
    final StringColumnMessage stringColumn = message.getStringColumn();
    startIndicesBytes = stringColumn.getStartIndices().asReadOnlyByteBuffer();
    startIndices = startIndicesBytes.asIntBuffer();
    endIndicesBytes = stringColumn.getEndIndices().asReadOnlyByteBuffer();
    endIndices = endIndicesBytes.asIntBuffer();
    data = new StringBuilder(stringColumn.getData().toStringUtf8());
    numStrings = message.getNumTuples();
  }

  /**
   * Constructs an empty column that can hold up to TupleBatch.BATCH_SIZE elements, but uses the averageStringSize to
   * seed the initial size of the internal buffer that stores the variable-length Strings.
   * 
   * @param averageStringSize expected average size of the Strings that will be stored in this column.
   */
  public StringColumn(final int averageStringSize) {
    startIndicesBytes = ByteBuffer.allocate(TupleBatch.BATCH_SIZE * (Integer.SIZE / Byte.SIZE));
    startIndices = startIndicesBytes.asIntBuffer();
    endIndicesBytes = ByteBuffer.allocate(TupleBatch.BATCH_SIZE * (Integer.SIZE / Byte.SIZE));
    endIndices = endIndicesBytes.asIntBuffer();
    data = new StringBuilder(averageStringSize * TupleBatch.BATCH_SIZE);
    numStrings = 0;
  }

  @Override
  public String get(final int row) {
    return getString(row);
  }

  @Override
  public void getIntoJdbc(final int row, final PreparedStatement statement, final int jdbcIndex) throws SQLException {
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

  /**
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   * @return this column.
   */
  public StringColumn put(final String value) {
    startIndices.put(data.length());
    data.append(value);
    endIndices.put(data.length());
    numStrings++;
    return this;
  }

  @Override
  public Type getType() {
    return Type.STRING_TYPE;
  }

  @Override
  public Column<String> putFromJdbc(final ResultSet resultSet, final int jdbcIndex) throws SQLException {
    return put(resultSet.getString(jdbcIndex));
  }

  @Override
  public void putFromSQLite(final SQLiteStatement statement, final int index) throws SQLiteException {
    put(statement.columnString(index));
  }

  @Override
  public Column<String> putObject(final Object value) {
    return put((String) value);
  }

  @Override
  public ColumnMessage serializeToProto() {
    /* Note that we do *not* build the inner class. We pass its builder instead. */
    final StringColumnMessage.Builder inner =
        StringColumnMessage.newBuilder().setData(ByteString.copyFromUtf8(data.toString()));
    inner.setStartIndices(ByteString.copyFrom(startIndicesBytes));
    inner.setEndIndices(ByteString.copyFrom(endIndicesBytes));
    return ColumnMessage.newBuilder().setType(ColumnMessageType.STRING).setNumTuples(size()).setStringColumn(inner)
        .build();
  }

  @Override
  public int size() {
    return numStrings;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(size()).append(" elements: [");
    for (int i = 0; i < size(); ++i) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(getString(i));
    }
    sb.append(']');
    return sb.toString();
  }
}