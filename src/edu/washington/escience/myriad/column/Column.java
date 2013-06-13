package edu.washington.escience.myriad.column;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;

/**
 * A column of a batch of tuples.
 * 
 * @param <T> type of the objects in this column.
 * 
 * @author dhalperi
 * 
 */
public interface Column<T extends Comparable<T>> {

  /**
   * Returns the element at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   */
  T get(int row);

  /**
   * Inserts the value in the specified row into the JDBC PreparedStatement at the specified index.
   * 
   * @param row position in this column of the specified element.
   * @param statement destination JDBC PreparedStatement.
   * @param jdbcIndex index in the JDBC PreparedStatement where the element should be placed. 1-indexed.
   * @throws SQLException if there are JDBC errors.
   */
  void getIntoJdbc(int row, PreparedStatement statement, int jdbcIndex) throws SQLException;

  /**
   * Inserts the value in the specified row into the SQLiteStatement at the specified index.
   * 
   * @param row position in this column of the specified element.
   * @param statement destination SQLiteStatement.
   * @param sqliteIndex index in the SQLiteStatement where the element should be placed. 1-indexed.
   * @throws SQLiteException if there are SQLite errors.
   */
  void getIntoSQLite(int row, SQLiteStatement statement, int sqliteIndex) throws SQLiteException;

  /**
   * @return a Myria {@link Type} object explaining what type of data is in this column.
   */
  Type getType();

  /**
   * Serializes this column as a protobuf message into the specified output stream.
   * 
   * @return a ColumnMessage containing a serialized copy of this column.
   */
  ColumnMessage serializeToProto();

  /**
   * Returns the number of elements in this column.
   * 
   * @return the number of elements in this column.
   */
  int size();

  /**
   * Check whether the value indexed by leftIdx in this column is equal to the value of the column rightColumn indexed
   * by rightIdx.
   * 
   * @param leftIdx the index on this column
   * @param rightColumn the right column
   * @param rightIdx the index of the value to compare with on the right column
   * @return true if equals, false otherwise
   */
  boolean equals(final int leftIdx, final Column<?> rightColumn, final int rightIdx);

  /**
   * Append the value indexed by leftIdx into the column builder.
   * 
   * @param index the index on this column
   * @param columnBuilder the column builder to append the value
   */
  void append(final int index, final ColumnBuilder<?> columnBuilder);
}