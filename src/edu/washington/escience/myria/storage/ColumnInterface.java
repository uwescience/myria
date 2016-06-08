package edu.washington.escience.myria.storage;

import edu.washington.escience.myria.Type;

/**
 * An interface for an object holding a single column of tuples.
 */
public interface ColumnInterface {
  /**
   * @return a Myria {@link Type} object explaining what type of data is in this column.
   */
  Type getType();

  /**
   * Returns the number of elements in this column.
   *
   * @return the number of elements in this column.
   */
  int size();
}
