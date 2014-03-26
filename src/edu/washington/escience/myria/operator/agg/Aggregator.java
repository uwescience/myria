package edu.washington.escience.myria.operator.agg;

import java.io.Serializable;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.ReadableColumn;
import edu.washington.escience.myria.storage.ReadableTable;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

/**
 * Single column aggregator.
 * 
 * @param <COLUMN_TYPE> the aggregate column type
 */
public interface Aggregator<COLUMN_TYPE> extends Serializable {

  /**
   * count. Count result is always of type {@link Type#LONG_TYPE}.
   */
  int AGG_OP_COUNT = 0x01;
  /**
   * min. Min result is always of the same type as the computed column.
   */
  int AGG_OP_MIN = 0x02;
  /**
   * max. Max result is always of the same type as the computed column.
   */
  int AGG_OP_MAX = 0x04;
  /**
   * sum. The sum is always the biggest similar type, e.g., INT->LONG and FLOAT->DOUBLE.
   */
  int AGG_OP_SUM = 0x08;
  /**
   * avg. All avg aggregates are of {@link Type#DOUBLE_TYPE} type.
   */
  int AGG_OP_AVG = 0x10;
  /**
   * stdev. All stdev aggregates are of double type
   */
  int AGG_OP_STDEV = 0x20;

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
   * @param t the source {@link ReadableTable}
   * @param column the column in <code>t</code> containing the value
   * @param row the row in <code>t</code> containing the value
   */
  void add(ReadableTable t, int column, int row);

  /**
   * Add a new value into the aggregate.
   * 
   * @param value the new value.
   */
  void add(COLUMN_TYPE value);

  /**
   * Add an object.
   * 
   * @param obj the object value.
   */
  void addObj(Object obj);

  /**
   * @return available aggregates this Aggregator supports.
   */
  int availableAgg();

  /**
   * Output the aggregate result. Store the output to buffer.
   * 
   * @param buffer the buffer to store the aggregate result.
   * @param fromIndex from the fromIndex to put the result columns
   */
  void getResult(TupleBatchBuffer buffer, int fromIndex);

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
