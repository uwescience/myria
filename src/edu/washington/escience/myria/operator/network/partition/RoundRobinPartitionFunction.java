package edu.washington.escience.myria.operator.network.partition;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.washington.escience.myria.storage.TupleBatch;

/**
 * A partition function that simply sends one tuple to each output in turn.
 *
 *
 */
public final class RoundRobinPartitionFunction extends PartitionFunction {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The next partition to use. */
  private int partition = 0;

  /**
   * @param numPartitions the number of partitions.
   */
  @JsonCreator
  public RoundRobinPartitionFunction(
      @Nullable @JsonProperty("numPartitions") final Integer numPartitions) {
    super(numPartitions);
  }

  @Override
  public int[] partition(@Nonnull final TupleBatch tb) {
    final int[] result = new int[tb.numTuples()];
    for (int i = 0; i < result.length; i++) {
      result[i] = partition;
      partition = (partition + 1) % numPartition();
    }
    return result;
  }
}
