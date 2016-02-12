package edu.washington.escience.myria.operator.network.distribute;

import java.io.Serializable;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.storage.TupleBatch;

/** The ShuffleProducer class uses an instance of the PartitionFunction class to decide which worker a tuple should be
 * routed to. Typically, the ShuffleProducer class invokes {@link #partition(Tuple, Schema) partition} on every tuple it
 * generates. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
  @Type(value = RoundRobinPartitionFunction.class, name = "RoundRobin"),
  @Type(value = IdentityPartitionFunction.class, name = "Identity"),
  @Type(value = HyperCubePartitionFunction.class, name = "HyperCube"),
  @Type(value = HashPartitionFunction.class, name = "Hash"),
  @Type(value = SinglePartitionFunction.class, name = "Single")
})
public abstract class PartitionFunction implements Serializable {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** number of partitions. */
  private Integer numPartitions = null;

  /** @return the number of partitions. */
  public final int numPartitions() {
    Preconditions.checkState(numPartitions != null, "numPartitions has not been set");
    return numPartitions;
  }

  /** partition the tuple batch into TupleBatch[], each element is one partition.
   *
   * @param data the data to be partitioned.
   * @return an array of partitions. */
  public abstract TupleBatch[] partition(@Nonnull final TupleBatch data);

  /** @param numPartitions the number of partitions. */
  public final void setNumPartitions(final int numPartitions) {
    Preconditions.checkArgument(numPartitions > 0, "numPartitions must be > 0");
    this.numPartitions = numPartitions;
  }
}
