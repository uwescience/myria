package edu.washington.escience.myria;

import org.joda.time.DateTime;

/**
 * Interface for relations.
 */
public interface Relation {

  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  boolean getBoolean(int column, int row);

  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  double getDouble(int column, int row);

  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  float getFloat(int column, int row);

  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  int getInt(int column, int row);

  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  long getLong(int column, int row);

  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  Object getObject(int column, int row);

  /**
   * Returns the Schema of the tuples in this batch.
   * 
   * @return the Schema of the tuples in this batch.
   */
  Schema getSchema();

  /**
   * Returns the element at the specified column and row position.
   * 
   * @param column column in which the element is stored.
   * @param row row in which the element is stored.
   * @return the element at the specified position in this TupleBatch.
   */
  String getString(int column, int row);

  /**
   * Returns the element at the specified column and row position.
   * 
   * @param column column in which the element is stored.
   * @param row row in which the element is stored.
   * @return the element at the specified position in this TupleBatch.
   */
  DateTime getDateTime(int column, int row);

  /**
   * The number of columns in this TupleBatch.
   * 
   * @return number of columns in this TupleBatch.
   */
  int numColumns();

  /**
   * Returns the number of valid tuples in this TupleBatch.
   * 
   * @return the number of valid tuples in this TupleBatch.
   */
  int numTuples();

}