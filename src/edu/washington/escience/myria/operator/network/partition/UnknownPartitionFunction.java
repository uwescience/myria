package edu.washington.escience.myria.operator.network.partition;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.washington.escience.myria.storage.TupleBatch;

/**
 * A partition function indicating that a dataset is partitioned by an unknown function.
 */
public final class UnknownPartitionFunction extends PartitionFunction {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * @param numPartitions the number of partitions.
   */
  @JsonCreator
  public UnknownPartitionFunction(@Nullable @JsonProperty("numPartitions") final Integer numPartitions) {
    super(numPartitions);
  }

  @Override
  public int[] partition(@Nonnull final TupleBatch tb) {
    throw new UnsupportedOperationException("Do not use UnknownPartitionFunction to partition a tuple batch");
  }
}
