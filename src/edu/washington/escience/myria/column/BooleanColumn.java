package edu.washington.escience.myria.column;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.BitSet;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hasher;
import com.google.protobuf.ByteString;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.builder.BooleanColumnBuilder;
import edu.washington.escience.myria.column.builder.ColumnBuilder;
import edu.washington.escience.myria.proto.DataProto.BooleanColumnMessage;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.util.ImmutableBitSet;
import edu.washington.escience.myria.util.ImmutableIntArray;

/**
 * A column of Boolean values. To save space, this implementation uses a BitSet as the internal representation.
 * 
 * @author dhalperi
 * 
 */
public final class BooleanColumn extends Column<Boolean> {
  /** Required for Java Serialization. */
  private static final long serialVersionUID = 1L;
  /** Internal representation of the column data. */
  private final BitSet data;
  /** Number of valid elements. */
  private final int numBits;

  /**
   * @param data the data
   * @param size the size of this column;
   * */
  public BooleanColumn(final BitSet data, final int size) {
    this.data = new ImmutableBitSet(data);
    numBits = size;
  }

  @Override
  public Boolean getObject(final int row) {
    return Boolean.valueOf(getBoolean(row));
  }

  /**
   * Returns the element at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   */
  @Override
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
    /* In SQLite, booleans are integers represented as 0 (false) or 1 (true). */
    int value = 0;
    if (getBoolean(row)) {
      value = 1;
    }
    statement.bind(sqliteIndex, value);
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
  public ColumnMessage serializeToProto(final ImmutableIntArray validIndices) {
    /* Construct the output ByteString using builder syntax. */
    ByteString.Output bytes = ByteString.newOutput((validIndices.length() + 7) / 8);
    int bitCnt = 0;
    int b = 0;
    for (int i : validIndices) {
      if (data.get(i)) {
        b |= (1 << bitCnt);
      }
      bitCnt++;
      if (bitCnt == 8) {
        bytes.write(b);
        bitCnt = 0;
        b = 0;
      }
    }
    /* Note that we do *not* build the inner class. We pass its builder instead. */
    final BooleanColumnMessage.Builder inner = BooleanColumnMessage.newBuilder().setData(bytes.toByteString());
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
    return getBoolean(leftIdx) == rightColumn.getBoolean(rightIdx);
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