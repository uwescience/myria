package edu.washington.escience.myria.operator.agg;

import java.io.Serializable;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.ReadableColumn;
import edu.washington.escience.myria.storage.ReadableTable;

/**
 * Single column aggregator.
 */
public interface PrimitiveAggregator extends Serializable {

  /**
   * The different aggregations that can be used when aggregating built-in types.
   */
  public enum AggregationOp {
    /** COUNT. Applies to all types. Result is always of type {@link Type#LONG_TYPE}. */
    COUNT,
    /** MIN. Applies to all types. Result is same as input type. */
    MIN,
    /** MAX. Applies to all types. Result is same as input type. */
    MAX,
    /**
     * SUM. Applies to numeric types. Result is the bigger numeric type, i.e., {@link Type#INT_TYPE} ->
     * {@link Type#LONG_TYPE} and . {@link Type#FLOAT_TYPE} -> {@link Type#DOUBLE_TYPE}.
     */
    SUM,
    /** AVG. Applies to numeric types. Result is always {@link Type#DOUBLE_TYPE}. */
    AVG,
    /** STDEV. Applies to numeric types. Result is always {@link Type#DOUBLE_TYPE}. */
    STDEV
  };

  /**
   * Add the entire contents of {@link ReadableColumn} into the aggregate.
   * 
   * @param from the source {@link ReadableColumn}
   */
  void add(ReadableColumn from);

  /**
   * Add the entire contents of the specified column from the {@link ReadableTable} into the aggregate.
   * 
   * @param from the source {@link ReadableTable}
   * @param fromColumn the column in the table to add values from
   */
  void add(ReadableTable from, int fromColumn);

  /**
   * Add the value in the specified <code>column</code> and <code>row</code> in the given {@link ReadableTable} into the
   * aggregate.
   * 
   * @param table the source {@link ReadableTable}
   * @param column the column in <code>t</code> containing the value
   * @param row the row in <code>t</code> containing the value
   */
  void add(ReadableTable table, int column, int row);

  /**
   * Output the aggregate result. Store the output to buffer.
   * 
   * @param dest the buffer to store the aggregate result.
   * @param destColumn from the fromIndex to put the result columns
   */
  void getResult(AppendableTable dest, int destColumn);

  /**
   * All the count aggregates are of type Long. All the avg aggregates are of type Double. And each of the max/min/sum
   * aggregate has the same type as the column on which the aggregate is computed.
   * 
   * @return Result schema of this Aggregator.
   */
  Schema getResultSchema();

  /**
   * @return The {@link Type} of the values this aggreagtor handles.
   */
  Type getType();
}
