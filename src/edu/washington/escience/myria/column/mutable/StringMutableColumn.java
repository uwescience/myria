package edu.washington.escience.myria.column.mutable;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hasher;
import com.google.protobuf.ByteString;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.StringArrayColumn;
import edu.washington.escience.myria.column.StringColumn;
import edu.washington.escience.myria.column.builder.ColumnBuilder;
import edu.washington.escience.myria.column.builder.StringColumnBuilder;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.proto.DataProto.StringColumnMessage;
import edu.washington.escience.myria.util.ImmutableIntArray;
import edu.washington.escience.myria.util.TypeFunnel;

/**
 * A mutable column of String values.
 * 
 */
public final class StringMutableColumn implements MutableColumn<String> {
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  /** Contains the packed character data. */
  private final String[] data;
  /** Number of elements in this column. */
  private final int numStrings;

  /**
   * Constructs a new column.
   * 
   * @param data the data
   * @param numStrings number of tuples.
   * */
  public StringMutableColumn(final String[] data, final int numStrings) {
    this.data = data;
    this.numStrings = numStrings;
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

  @Override
  public Type getType() {
    return Type.STRING_TYPE;
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
  public ColumnMessage serializeToProto(final ImmutableIntArray validIndices) {
    /* Note that we do *not* build the inner class. We pass its builder instead. */
    final StringColumnMessage.Builder inner = StringColumnMessage.newBuilder();
    StringBuilder sb = new StringBuilder();
    int startP = 0, endP = 0;
    for (int i : validIndices) {
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

  @Override
  public boolean equals(final int leftIdx, final Column<?> rightColumn, final int rightIdx) {
    return getString(leftIdx).equals(rightColumn.get(rightIdx));
  }

  @Override
  public void append(final int index, final ColumnBuilder<?> columnBuilder) {
    ((StringColumnBuilder) columnBuilder).append(getString(index));
  }

  @Override
  public void addToHasher(final int row, final Hasher hasher) {
    hasher.putObject(getString(row), TypeFunnel.INSTANCE);
  }

  @Override
  public void replace(final int index, final String value) {
    Preconditions.checkElementIndex(index, size());
    data[index] = value;
  }

  @Override
  public StringColumn toColumn() {
    return new StringArrayColumn(data.clone(), numStrings);
  }

  @Override
  public StringMutableColumn clone() {
    return new StringMutableColumn(data.clone(), numStrings);
  }
}