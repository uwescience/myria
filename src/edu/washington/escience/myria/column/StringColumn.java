package edu.washington.escience.myria.column;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.hash.Hasher;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.builder.ColumnBuilder;
import edu.washington.escience.myria.column.builder.StringColumnBuilder;
import edu.washington.escience.myria.util.TypeFunnel;

/**
 * An abstract column of String values.
 * 
 */
public abstract class StringColumn implements Column<String> {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  @Override
  public final String get(final int row) {
    return getString(row);
  }

  @Override
  public final void getIntoJdbc(final int row, final PreparedStatement statement, final int jdbcIndex)
      throws SQLException {
    statement.setString(jdbcIndex, getString(row));
  }

  @Override
  public final void getIntoSQLite(final int row, final SQLiteStatement statement, final int sqliteIndex)
      throws SQLiteException {
    statement.bind(sqliteIndex, getString(row));
  }

  /**
   * Returns the element at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   */
  public abstract String getString(final int row);

  @Override
  public final Type getType() {
    return Type.STRING_TYPE;
  }

  @Override
  public final String toString() {
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
  public final boolean equals(final int leftIdx, final Column<?> rightColumn, final int rightIdx) {
    return getString(leftIdx).equals(rightColumn.get(rightIdx));
  }

  @Override
  public final void append(final int index, final ColumnBuilder<?> columnBuilder) {
    ((StringColumnBuilder) columnBuilder).append(getString(index));
  }

  @Override
  public final void addToHasher(final int row, final Hasher hasher) {
    hasher.putObject(getString(row), TypeFunnel.INSTANCE);
  }
}