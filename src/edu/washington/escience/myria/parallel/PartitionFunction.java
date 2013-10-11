package edu.washington.escience.myria.parallel;

import java.io.Serializable;

import edu.washington.escience.myria.TupleBatch;

/**
 * The ShuffleProducer class uses an instance of the PartitionFunction class to decide which worker a tuple should be
 * routed to. Typically, the ShuffleProducer class invokes {@link partition(Tuple, Schema) partition} on every tuple it
 * generates.
 */
public abstract class PartitionFunction implements Serializable {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * number of partitions.
   * */
  private final int numPartition;

  /**
   * Each partition function implementation must has a Class(int) style constructor.
   * 
   * @param numPartition number of partitions.
   */
  public PartitionFunction(final int numPartition) {
    this.numPartition = numPartition;
  }

  /**
   * @return the number of partitions.
   * */
  public final int numPartition() {
    return numPartition;
  }

  /**
   * Given that the TupleBatches expose only the valid tuples, partition functions using TB.get** methods should be of
   * little overhead comparing with direct Column access .
   * 
   * @param data data TupleBatch.
   * 
   * @return the partition
   * 
   */
  public abstract int[] partition(TupleBatch data);
}
