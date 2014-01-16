package edu.washington.escience.myria.column;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.joda.time.DateTime;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.hash.Hasher;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.builder.ColumnBuilder;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.util.ImmutableIntArray;

/**
 * A column of a batch of tuples.
 * 
 * @param <T> type of the objects in this column.
 * 
 * @author dhalperi
 * 
 */
public abstract class Column<T extends Comparable<?>> implements Serializable {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * Adds the value in the row to a hasher object.
   * 
   * @param row the row in this column
   * @param hasher the hasher object to add the value
   */
  public abstract void addToHasher(final int row, final Hasher hasher);

  /**
   * Append the value indexed by leftIdx into the column builder.
   * 
   * @param index the index on this column
   * @param columnBuilder the column builder to append the value
   */
  public abstract void append(final int index, final ColumnBuilder<?> columnBuilder);

  /**
   * Check whether the value indexed by leftIdx in this column is equal to the value of the column rightColumn indexed
   * by rightIdx.
   * 
   * @param leftIdx the index on this column
   * @param rightColumn the right column
   * @param rightIdx the index of the value to compare with on the right column
   * @return true if equals, false otherwise
   */
  public abstract boolean equals(final int leftIdx, final Column<?> rightColumn, final int rightIdx);

  /**
   * Returns the boolean value at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   * @throws UnsupportedOperationException if this column does not support this type.
   */
  public boolean getBoolean(final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * Returns the {@link DateTime} value at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   * @throws UnsupportedOperationException if this column does not support this type.
   */
  public DateTime getDateTime(final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * Returns the double value at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   * @throws UnsupportedOperationException if this column does not support this type.
   */
  public double getDouble(final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * Returns the float value at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   * @throws UnsupportedOperationException if this column does not support this type.
   */
  public float getFloat(final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * Returns the int value at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   * @throws UnsupportedOperationException if this column does not support this type.
   */
  public int getInt(final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * Inserts the value in the specified row into the JDBC PreparedStatement at the specified index.
   * 
   * @param row position in this column of the specified element.
   * @param statement destination JDBC PreparedStatement.
   * @param jdbcIndex index in the JDBC PreparedStatement where the element should be placed. 1-indexed.
   * @throws SQLException if there are JDBC errors.
   */
  public abstract void getIntoJdbc(int row, PreparedStatement statement, int jdbcIndex) throws SQLException;

  /**
   * Inserts the value in the specified row into the SQLiteStatement at the specified index.
   * 
   * @param row position in this column of the specified element.
   * @param statement destination SQLiteStatement.
   * @param sqliteIndex index in the SQLiteStatement where the element should be placed. 1-indexed.
   * @throws SQLiteException if there are SQLite errors.
   */
  public abstract void getIntoSQLite(int row, SQLiteStatement statement, int sqliteIndex) throws SQLiteException;

  /**
   * Returns the long value at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   * @throws UnsupportedOperationException if this column does not support this type.
   */
  public long getLong(final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * Returns the element at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   */
  public abstract T getObject(int row);

  /**
   * Returns the {@link String} value at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   * @throws UnsupportedOperationException if this column does not support this type.
   */
  public String getString(final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @return a Myria {@link Type} object explaining what type of data is in this column.
   */
  public abstract Type getType();

  /**
   * Serializes this column as a protobuf message into the specified output stream.
   * 
   * @return a ColumnMessage containing a serialized copy of this column.
   */
  public abstract ColumnMessage serializeToProto();

  /**
   * Serializes this column as a protobuf message into the specified output stream.
   * 
   * @param validIndices the rows of the column to serialize.
   * @return a ColumnMessage containing a serialized copy of this column.
   */
  public abstract ColumnMessage serializeToProto(ImmutableIntArray validIndices);

  /**
   * Returns the number of elements in this column.
   * 
   * @return the number of elements in this column.
   */
  public abstract int size();
}