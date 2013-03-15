package edu.washington.escience.myriad.column;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.BitSet;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.proto.DataProto.BooleanColumnMessage;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage.ColumnMessageType;

/**
 * A column of Boolean values. To save space, this implementation uses a BitSet as the internal representation.
 * 
 * @author dhalperi
 * 
 */
public final class BooleanColumn implements Column<Boolean> {
  /** Internal representation of the column data. */
  private final BitSet data;
  /** Number of valid elements. */
  private int numBits;

  /** Constructs an empty column that can hold up to TupleBatch.BATCH_SIZE elements. */
  public BooleanColumn() {
    data = new BitSet(TupleBatch.BATCH_SIZE);
    numBits = 0;
  }

  /**
   * Constructs a BooleanColumn by deserializing the given ColumnMessage.
   * 
   * @param message a ColumnMessage containing the contents of this column.
   * @param numTuples num tuples in the column message
   */
  public BooleanColumn(final ColumnMessage message, final int numTuples) {
    if (message.getType().ordinal() != ColumnMessageType.BOOLEAN_VALUE) {
      throw new IllegalArgumentException("Trying to construct BooleanColumn from non-BOOLEAN ColumnMessage");
    }
    if (!message.hasBooleanColumn()) {
      throw new IllegalArgumentException("ColumnMessage has type BOOLEAN but no BooleanColumn");
    }
    data = BitSet.valueOf(message.getBooleanColumn().getData().asReadOnlyByteBuffer());
    numBits = numTuples;
  }

  @Override
  public Boolean get(final int row) {
    return Boolean.valueOf(getBoolean(row));
  }

  /**
   * Returns the element at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   */
  public boolean getBoolean(final int row) {
    Preconditions.checkElementIndex(row, numBits);
    return data.get(row);
  }

  @Override
  public void getIntoJdbc(final int row, final PreparedStatement statement, final int jdbcIndex) throws SQLException {
    statement.setBoolean(jdbcIndex, getBoolean(row));
  }

  @Override
  public void getIntoSQLite(final int row, final SQLiteStatement statement, final int sqliteIndex)
      throws SQLiteException {
    throw new UnsupportedOperationException("SQLite does not support Boolean columns.");
  }

  /**
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   * @return this column.
   */
  public BooleanColumn put(final boolean value) {
    Preconditions.checkElementIndex(numBits, TupleBatch.BATCH_SIZE);
    data.set(numBits, value);
    numBits++;
    return this;
  }

  @Override
  public Column<Boolean> putFromJdbc(final ResultSet resultSet, final int jdbcIndex) throws SQLException {
    return put(resultSet.getBoolean(jdbcIndex));
  }

  @Override
  public void putFromSQLite(final SQLiteStatement statement, final int index) throws SQLiteException {
    throw new UnsupportedOperationException("SQLite does not support Boolean columns.");
  }

  @Override
  public Column<Boolean> putObject(final Object value) {
    return put((Boolean) value);
  }

  @Override
  public ColumnMessage serializeToProto() {
    /* Note that we do *not* build the inner class. We pass its builder instead. */
    final BooleanColumnMessage.Builder inner =
        BooleanColumnMessage.newBuilder().setData(ByteString.copyFrom(data.toByteArray()));
    return ColumnMessage.newBuilder().setType(ColumnMessageType.BOOLEAN).setBooleanColumn(inner).build();
  }

  @Override
  public int size() {
    return numBits;
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