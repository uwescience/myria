package edu.washington.escience.myria.column;

import java.nio.BufferOverflowException;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import edu.washington.escience.myria.Type;

/**
 * @param <T> type of the objects in this column.
 * 
 */
public interface ColumnBuilder<T extends Comparable<?>> {

  /**
   * @return a Myria {@link Type} object explaining what type of data is in this column.
   */
  Type getType();

  /**
   * Extracts the appropriate value from a JDBC ResultSet object and appends it to this column.
   * 
   * @param resultSet contains the results
   * @param jdbcIndex the position of the element to extract. 1-indexed.
   * @return this column builder.
   * @throws SQLException if there are JDBC errors.
   * @throws BufferOverflowException if the column is already full
   */
  ColumnBuilder<T> appendFromJdbc(ResultSet resultSet, int jdbcIndex) throws SQLException, BufferOverflowException;

  /**
   * Extracts the appropriate value from a SQLiteStatement object and appends it to this column.
   * 
   * @param statement contains the results
   * @return this column builder.
   * @param index the position of the element to extract. 0-indexed.
   * @throws SQLiteException if there are SQLite errors.
   * @throws BufferOverflowException if the column is already full
   */
  ColumnBuilder<T> appendFromSQLite(SQLiteStatement statement, int index) throws SQLiteException,
      BufferOverflowException;

  /**
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   * @return this column builder.
   * @throws BufferOverflowException if the column is already full
   */
  ColumnBuilder<T> append(T value) throws BufferOverflowException;

  /**
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   * @return this column builder.
   * @throws BufferOverflowException if the column is already full
   */
  ColumnBuilder<T> appendObject(Object value) throws BufferOverflowException;

  /**
   * Replace the specified element.
   * 
   * @param value element to be inserted.
   * @param idx where to insert the element.
   * @return this column builder.
   * @throws IndexOutOfBoundsException if the idx exceeds the currently valid indices, i.e. the currently built size.
   */
  ColumnBuilder<T> replace(int idx, T value) throws IndexOutOfBoundsException;

  /**
   * expand some size.
   * 
   * @param size to expand
   * @return this column builder.
   * @throws BufferOverflowException if expanding size exceeds the column capacity
   * */
  ColumnBuilder<T> expand(int size) throws BufferOverflowException;

  /**
   * expand to full size.
   * 
   * @return this column builder.
   * */
  ColumnBuilder<T> expandAll();

  /**
   * @return the number of elements in the current building column.
   */
  int size();

  /**
   * @return a column with the contents built.
   * */
  Column<T> build();

  /**
   * @return a new builder with the contents initialized by this builder. The two builders share no data.
   * */
  ColumnBuilder<T> forkNewBuilder();

  /**
   * Returns the current element at the specified row in this building column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   */
  T get(int row);

  void reset();

}