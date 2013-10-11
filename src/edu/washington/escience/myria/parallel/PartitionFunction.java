package edu.washington.escience.myria.parallel;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import edu.washington.escience.myria.TupleBatch;

/**
 * The ShuffleProducer class uses an instance of the PartitionFunction class to decide which worker a tuple should be
 * routed to. Typically, the ShuffleProducer class invokes {@link partition(Tuple, Schema) partition} on every tuple it
 * generates.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @Type(value = RoundRobinPartitionFunction.class, name = "RoundRobin"),
    @Type(value = MultiFieldHashPartitionFunction.class, name = "MultiFieldHash"),
    @Type(value = SingleFieldHashPartitionFunction.class, name = "SingleFieldHash") })
public abstract class PartitionFunction implements Serializable {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * number of partitions.
   * */
  private Integer numPartitions;

  /**
   * Each partition function implementation must has a Class(int) style constructor.
   * 
   * @param numPartitions number of partitions.
   */
  public PartitionFunction(final Integer numPartitions) {
    this.numPartitions = numPartitions;
  }

  /**
   * @return the number of partitions.
   */
  public final int numPartition() {
    return numPartitions;
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
  public abstract int[] partition(final TupleBatch data);

  /**
   * Set the number of output partitions.
   * 
   * @param numPartitions the number of output partitions.
   */
  public final void setNumPartitions(final int numPartitions) {
    this.numPartitions = numPartitions;
  }
}
