package edu.washington.escience.myria.parallel;

import java.io.Serializable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.TupleBatch;

/**
 * The ShuffleProducer class uses an instance of the PartitionFunction class to decide which worker a tuple should be
 * routed to. Typically, the ShuffleProducer class invokes {@link partition(Tuple, Schema) partition} on every tuple it
 * generates.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @Type(value = RoundRobinPartitionFunction.class, name = "RoundRobin"),
    @Type(value = SingleFieldHashPartitionFunction.class, name = "SingleFieldHash"),
    @Type(value = MultiFieldHashPartitionFunction.class, name = "MultiFieldHash"),
    @Type(value = WholeTupleHashPartitionFunction.class, name = "WholeTupleHash") })
public abstract class PartitionFunction implements Serializable {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** Invalid number of partitions. */
  private static final int INVALID_NUM_PARTITIONS = -1;

  /**
   * The number of partitions into which input tuples can be divided.
   */
  private int numPartitions = INVALID_NUM_PARTITIONS;

  /**
   * @param numPartitions the number of partitions into which input tuples can be divided. Note that this is a
   *          {@link Integer} not an {@link int} so that it can properly handle <code>null</code> values, e.g., in JSON
   *          deserialization.
   */
  public PartitionFunction(@Nullable final Integer numPartitions) {
    this.numPartitions = Objects.firstNonNull(numPartitions, INVALID_NUM_PARTITIONS);
  }

  /**
   * @return the number of partitions.
   */
  public final int numPartition() {
    if (numPartitions == INVALID_NUM_PARTITIONS) {
      throw new IllegalStateException("numPartitions has not been set");
    }
    return numPartitions;
  }

  /**
   * Given that the TupleBatches expose only the valid tuples, partition functions using TB.get** methods should be of
   * little overhead comparing with direct Column access.
   * 
   * @param data the data to be partitioned.
   * 
   * @return an int[] of length specified by <code>data.{@link TupleBatch#numTuples}</code>, specifying which partition
   *         every tuple should be sent to.
   * 
   */
  public abstract int[] partition(@Nonnull final TupleBatch data);

  /**
   * Set the number of output partitions.
   * 
   * @param numPartitions the number of output partitions. Must be greater than 0.
   */
  public final void setNumPartitions(final int numPartitions) {
    Preconditions.checkArgument(numPartitions > 0, "numPartitions must be > 0");
    this.numPartitions = numPartitions;
  }
}
