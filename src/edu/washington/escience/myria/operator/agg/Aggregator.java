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
   * @param state the initial state of the aggregate, which will be mutated.
   * @throws DbException if there is an error.
   */
  void add(ReadableTable from, Object state) throws DbException;

  /**
   * Update this aggregate using the specified row of the specified table.
   *
   * @param from the source {@link ReadableTable}.
   * @param row the specified row.
   * @param state the initial state of the aggregate, which will be mutated.
   * @throws DbException if there is an error.
   */
  void addRow(ReadableTable from, int row, Object state) throws DbException;

  /**
   * Append the aggregate result(s) to the given table starting from the given column.
   *
   * @param dest where to store the aggregate result.
   * @param destColumn the starting index into which aggregates will be output.
   * @param state the initial state of the aggregate, which will be mutated.
   * @throws DbException if there is an error.
   */
  void getResult(AppendableTable dest, int destColumn, Object state) throws DbException;

  /**
   * Compute and return the initial state tuple for instances of this {@link Aggregator}.
   *
   * @return the initial state tuple for instances of this {@link Aggregator}.
   */
  Object getInitialState();

  /**
   * Compute and return the schema of the outputs of this {@link Aggregator}.
   *
   * @return the schema of the outputs of this {@link Aggregator}.
   */
  Schema getResultSchema();
}
