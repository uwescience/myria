package edu.washington.escience.myriad.column;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.protobuf.Message;

/**
 * A column of a batch of tuples.
 * 
 * @author dhalperi
 * 
 */
public interface Column {

  /**
   * Returns the element at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   */
  Object get(int row);

  /**
   * Inserts the value in the specified row into the JDBC PreparedStatement at the specified index.
   * 
   * @param row position in this column of the specified element.
   * @param statement destination JDBC PreparedStatement.
   * @param jdbcIndex index in the JDBC PreparedStatement where the element should be placed.
   *          1-indexed.
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
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   */
  void put(Object value);

  /**
   * Extracts the appropriate value from a JDBC ResultSet object and appends it to this column.
   * 
   * @param resultSet contains the results
   * @param jdbcIndex the position of the element to extract. 1-indexed.
   * @throws SQLException if there are JDBC errors.
   */
  void putFromJdbc(ResultSet resultSet, int jdbcIndex) throws SQLException;

  /**
   * Extracts the appropriate value from a SQLiteStatement object and appends it to this column.
   * 
   * @param statement contains the results
   * @param index the position of the element to extract. 0-indexed.
   * @throws SQLiteException if there are SQLite errors.
   */
  void putFromSQLite(SQLiteStatement statement, int index) throws SQLiteException;

  /**
   * Serializes this column as a protobuf message into the specified output stream.
   * 
   * @param output the output stream.
   * @throws IOException if there are ProtoBuf errors.
   */
  Message serializeToProto() throws IOException;

  /**
   * Returns the number of elements in this column.
   * 
   * @return the number of elements in this column.
   */
  int size();
}