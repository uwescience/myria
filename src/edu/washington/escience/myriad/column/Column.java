package edu.washington.escience.myriad.column;

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
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   */
  void put(Object value);

  /**
   * Returns the number of elements in this column.
   * 
   * @return the number of elements in this column.
   */
  int size();
}