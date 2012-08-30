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
import edu.washington.escience.myriad.proto.TransportProto.BooleanColumnMessage;
import edu.washington.escience.myriad.proto.TransportProto.ColumnMessage;
import edu.washington.escience.myriad.proto.TransportProto.ColumnMessage.ColumnMessageType;

/**
 * A column of Boolean values. To save space, this implementation uses a BitSet as the internal
 * representation.
 * 
 * @author dhalperi
 * 
 */
public final class BooleanColumn implements Column {
  /** Internal representation of the column data. */
  private final BitSet data;
  /** Number of valid elements. */
  private int numBits;

  /** Constructs an empty column that can hold up to TupleBatch.BATCH_SIZE elements. */
  public BooleanColumn() {
    this.data = new BitSet(TupleBatch.BATCH_SIZE);
    this.numBits = 0;
  }

  /**
   * Constructs a BooleanColumn by deserializing the given ColumnMessage.
   * 
   * @param message a ColumnMessage containing the contents of this column.
   */
  public BooleanColumn(final ColumnMessage message) {
    if (message.getType().ordinal() != ColumnMessageType.BOOLEAN_VALUE) {
      throw new IllegalArgumentException(
          "Trying to construct BooleanColumn from non-BOOLEAN ColumnMessage");
    }
    if (!message.hasBooleanColumn()) {
      throw new IllegalArgumentException("ColumnMessage has type BOOLEAN but no BooleanColumn");
    }
    this.data = BitSet.valueOf(message.getBooleanColumn().getData().asReadOnlyByteBuffer());
    this.numBits = message.getNumTuples();
  }

  @Override
  public Object get(final int row) {
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
  public void getIntoJdbc(final int row, final PreparedStatement statement, final int jdbcIndex)
      throws SQLException {
    statement.setBoolean(jdbcIndex, getBoolean(row));
  }

  @Override
  public void getIntoSQLite(final int row, final SQLiteStatement statement, final int sqliteIndex)
      throws SQLiteException {
    throw new UnsupportedOperationException("SQLite does not support Boolean columns.");
  }

  @Override
  public void put(final Object value) {
    putBoolean((Boolean) value);
  }

  /**
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   */
  public void putBoolean(final boolean value) {
    Preconditions.checkElementIndex(numBits, TupleBatch.BATCH_SIZE);
    data.set(numBits, value);
    numBits++;
  }

  @Override
  public void putFromJdbc(final ResultSet resultSet, final int jdbcIndex) throws SQLException {
    putBoolean(resultSet.getBoolean(jdbcIndex));
  }

  @Override
  public void putFromSQLite(final SQLiteStatement statement, final int index)
      throws SQLiteException {
    throw new UnsupportedOperationException("SQLite does not support Boolean columns.");
  }

  @Override
  public ColumnMessage serializeToProto() {
    /* Note that we do *not* build the inner class. We pass its builder instead. */
    BooleanColumnMessage.Builder inner =
        BooleanColumnMessage.newBuilder().setData(ByteString.copyFrom(data.toByteArray()));
    return ColumnMessage.newBuilder().setType(ColumnMessageType.BOOLEAN).setNumTuples(size())
        .setBooleanColumn(inner).build();
  }

  @Override
  public int size() {
    return numBits;
  }
}