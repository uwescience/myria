package edu.washington.escience.myria.column.mutable;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.hash.Hasher;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.builder.ColumnBuilder;
import edu.washington.escience.myria.column.builder.IntColumnBuilder;

/**
 * An abstract MutableColumn<Integer> with a primitive type accessor.
 * 
 */
public abstract class IntMutableColumn implements MutableColumn<Integer> {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * Returns the element at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   */
  public abstract int getInt(final int row);

  @Override
  public final Type getType() {
    return Type.INT_TYPE;
  }

  @Override
  public final void getIntoJdbc(final int row, final PreparedStatement statement, final int jdbcIndex)
      throws SQLException {
    statement.setInt(jdbcIndex, getInt(row));
  }

  @Override
  public final void getIntoSQLite(final int row, final SQLiteStatement statement, final int sqliteIndex)
      throws SQLiteException {
    statement.bind(sqliteIndex, getInt(row));
  }

  @Override
  public final boolean equals(final int leftIdx, final Column<?> rightColumn, final int rightIdx) {
    return getInt(leftIdx) == ((IntMutableColumn) rightColumn).getInt(rightIdx);
  }

  @Override
  public final void append(final int index, final ColumnBuilder<?> columnBuilder) {
    ((IntColumnBuilder) columnBuilder).append(getInt(index));
  }

  @Override
  public final void addToHasher(final int row, final Hasher hasher) {
    hasher.putInt(getInt(row));
  }

  @Override
  public final String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(size()).append(" elements: [");
    for (int i = 0; i < size(); ++i) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(getInt(i));
    }
    sb.append(']');
    return sb.toString();
  }
}
