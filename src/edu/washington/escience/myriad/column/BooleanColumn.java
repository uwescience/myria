package edu.washington.escience.myriad.column;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.BitSet;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.TupleBatch;

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
  public int size() {
    return numBits;
  }
}