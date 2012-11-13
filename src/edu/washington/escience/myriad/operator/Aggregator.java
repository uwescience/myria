package edu.washington.escience.myriad.operator;

import java.io.Serializable;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.table._TupleBatch;

/**
 * Single field aggregator.
 */
public interface Aggregator extends Serializable {

  public static int AGG_OP_COUNT = 0x01;
  public static int AGG_OP_MIN = 0x02;
  public static int AGG_OP_MAX = 0x04;
  public static int AGG_OP_SUM = 0x08;
  /**
   * All avg aggregates are of double type.
   * */
  public static int AGG_OP_AVG = 0x10;

  /**
   * Add a new TupleBatch into the aggregate.
   */
  void add(_TupleBatch t);

  /**
   * @param buffer the buffer to store the aggregate result.
   * @param fromIndex from the fromIndex to put the result columns
   * */
  public void getResult(TupleBatchBuffer buffer, int fromIndex);

  public int available();

  public Schema getResultSchema();

}
