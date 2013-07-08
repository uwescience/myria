package edu.washington.escience.myriad.column;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.BitSet;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hasher;
import com.google.protobuf.ByteString;

import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.proto.DataProto.BooleanColumnMessage;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;
import edu.washington.escience.myriad.util.ImmutableBitSet;

/**
 * A column of Boolean values. To save space, this implementation uses a BitSet as the internal representation.
 * 
 * @author dhalperi
 * 
 */
public final class BooleanColumn implements Column<Boolean> {
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  /** Internal representation of the column data. */
  private final BitSet data;
  /** Number of valid elements. */
  private final int numBits;

  /**
   * @param data the data
   * @param size the size of this column;
   * */
  BooleanColumn(final BitSet data, final int size) {
    this.data = new ImmutableBitSet(data);
    numBits = size;
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

  @Override
  public Type getType() {
    return Type.BOOLEAN_TYPE;
  }

  @Override
  public ColumnMessage serializeToProto() {
    /* Note that we do *not* build the inner class. We pass its builder instead. */
    final BooleanColumnMessage.Builder inner =
        BooleanColumnMessage.newBuilder().setData(ByteString.copyFrom(data.toByteArray()));
    return ColumnMessage.newBuilder().setType(ColumnMessage.Type.BOOLEAN).setBooleanColumn(inner).build();
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
    sb.append(']');
    return sb.toString();
  }

  @Override
  public boolean equals(final int leftIdx, final Column<?> rightColumn, final int rightIdx) {
    return getBoolean(leftIdx) == ((BooleanColumn) rightColumn).getBoolean(rightIdx);
  }

  @Override
  public void append(final int index, final ColumnBuilder<?> columnBuilder) {
    ((BooleanColumnBuilder) columnBuilder).append(getBoolean(index));
  }

  @Override
  public void addToHasher(final int row, final Hasher hasher) {
    hasher.putBoolean(getBoolean(row));
  }
}