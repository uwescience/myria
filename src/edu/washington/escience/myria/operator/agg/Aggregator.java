package edu.washington.escience.myria.operator.agg;

import java.io.Serializable;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.ReadableTable;

/**
 * The interface for any aggregation.
 */
public interface Aggregator extends Serializable {

  /**
   * Update this aggregate using all rows of the specified table.
   * 
   * @param from the source {@link ReadableTable}.
   * @throws DbException if there is an error.
   */
  void add(ReadableTable from) throws DbException;

  /**
   * Update this aggregate using the specified of the specified table.
   * 
   * @param from the source {@link ReadableTable}.
   * @param row the specified row.
   * @throws DbException if there is an error.
   */
  void addRow(ReadableTable from, int row) throws DbException;

  /**
   * Append the aggregate result(s) to the given table starting from the given column.
   * 
   * @param dest where to store the aggregate result.
   * @param destColumn the starting index into which aggregates will be output.
   * @throws DbException if there is an error.
   */
  void getResult(AppendableTable dest, int destColumn) throws DbException;

  /**
   * Compute and return the schema of the outputs of this {@link Aggregator}.
   * 
   * @return the schema of the outputs of this {@link Aggregator}.
   */
  Schema getResultSchema();
}
