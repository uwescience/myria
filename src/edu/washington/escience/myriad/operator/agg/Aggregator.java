package edu.washington.escience.myriad.operator.agg;

import java.io.Serializable;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;

/**
 * Single column aggregator.
 */
public interface Aggregator extends Serializable {

  /**
   * count.
   * */
  int AGG_OP_COUNT = 0x01;
  /**
   * min.
   * */
  int AGG_OP_MIN = 0x02;
  /**
   * max.
   * */
  int AGG_OP_MAX = 0x04;
  /**
   * sum.
   * */
  int AGG_OP_SUM = 0x08;
  /**
   * avg. All avg aggregates are of double type.
   * */
  int AGG_OP_AVG = 0x10;
  /**
   * stdev. All stdev aggregates are of double type
   */
  int AGG_OP_STDEV = 0x20;

  /**
   * Add a new TupleBatch into the aggregate.
   * 
   * @param t
   *          TupleBatch
   */
  void add(TupleBatch t);

  /**
   * @return available aggregates this Aggregator supports.
   * */
  int availableAgg();

  /**
   * Copy an Aggregator without states.
   * 
   * @return an new Aggregator instance with the same configuration parameters
   *         as this instance but without states.
   * */
  Aggregator freshCopyYourself();

  /**
   * Output the aggregate result. Store the output to buffer.
   * 
   * @param buffer
   *          the buffer to store the aggregate result.
   * @param fromIndex
   *          from the fromIndex to put the result columns
   * */
  void getResult(TupleBatchBuffer buffer, int fromIndex);

  /**
   * All the count aggregates are of type Long. All the avg aggregates are of
   * type Double. And each of the max/min/sum aggregate has the same type as the
   * column on which the aggregate is computed.
   * 
   * @return Result schema of this Aggregator.
   * 
   * */
  Schema getResultSchema();

}
