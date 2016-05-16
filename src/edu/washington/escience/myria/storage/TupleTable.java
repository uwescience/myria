package edu.washington.escience.myria.storage;

import edu.washington.escience.myria.Schema;

/**
 * Any object that is a table (2-D) of tuples.
 *
 */
public interface TupleTable {
  /**
   * Returns the Schema of the tuples in this table.
   *
   * @return the Schema of the tuples in this table.
   */
  Schema getSchema();

  /**
   * The number of columns in this table.
   *
   * @return number of columns in this tableBatch.
   */
  int numColumns();

  /**
   * Returns the number of valid tuples in this table.
   *
   * @return the number of valid tuples in this table.
   */
  int numTuples();
}
