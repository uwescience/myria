package edu.washington.escience.myriad.column;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;
import edu.washington.escience.myriad.proto.DataProto.StringColumnMessage;

/**
 * A column of String values.
 * 
 * @author dhalperi
 * 
 */
public final class StringColumn implements Column<String> {
  /** Contains the packed character data. */
  private final String[] data;
  /** Number of elements in this column. */
  private int numStrings;

  /**
   * Constructs an empty column that can hold up to TupleBatch.BATCH_SIZE elements.
   * */
  public StringColumn() {
    data = new String[TupleBatch.BATCH_SIZE];
    numStrings = 0;
  }

  /**
   * Constructs a StringColumn by deserializing the given ColumnMessage.
   * 
   * @param message a ColumnMessage containing the contents of this column.
   * @param numTuples num tuples in the column message
   */
  public StringColumn(final ColumnMessage message, final int numTuples) {
    if (message.getType().ordinal() != ColumnMessage.Type.STRING_VALUE) {
      throw new IllegalArgumentException("Trying to construct StringColumn from non-STRING ColumnMessage");
    }
    if (!message.hasStringColumn()) {
      throw new IllegalArgumentException("ColumnMessage has type STRING but no StringColumn");
    }
    final StringColumnMessage stringColumn = message.getStringColumn();
    List<Integer> startIndices = stringColumn.getStartIndicesList();
    List<Integer> endIndices = stringColumn.getEndIndicesList();
    String[] newData = new String[numTuples];
    String allStrings = stringColumn.getData().toStringUtf8();
    for (int i = 0; i < numTuples; i++) {
      newData[i] = allStrings.substring(startIndices.get(i), endIndices.get(i));
    }
    data = newData;
    numStrings = numTuples;
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
    return data[row];
  }

  /**
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   * @return this column.
   */
  public StringColumn put(final String value) {
    data[numStrings] = value;
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
    final StringColumnMessage.Builder inner = StringColumnMessage.newBuilder();
    StringBuilder sb = new StringBuilder();
    int startP = 0, endP = 0;
    for (int i = 0; i < numStrings; i++) {
      endP = startP + data[i].length();
      inner.addStartIndices(startP);
      inner.addEndIndices(endP);
      sb.append(data[i]);
      startP = endP;
    }
    inner.setData(ByteString.copyFromUtf8(sb.toString()));

    return ColumnMessage.newBuilder().setType(ColumnMessage.Type.STRING).setStringColumn(inner).build();
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