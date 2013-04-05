package edu.washington.escience.myriad.column;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
   * Extracts the appropriate value from a JDBC ResultSet object and appends it to this column.
   * 
   * @param resultSet contains the results
   * @param jdbcIndex the position of the element to extract. 1-indexed.
   * @return this column.
   * @throws SQLException if there are JDBC errors.
   */
  Column<T> putFromJdbc(ResultSet resultSet, int jdbcIndex) throws SQLException;

  /**
   * Extracts the appropriate value from a SQLiteStatement object and appends it to this column.
   * 
   * @param statement contains the results
   * @param index the position of the element to extract. 0-indexed.
   * @throws SQLiteException if there are SQLite errors.
   */
  void putFromSQLite(SQLiteStatement statement, int index) throws SQLiteException;

  /**
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   * @return this column.
   */
  Column<T> putObject(Object value);

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
}