package edu.washington.escience.myria.operator.network.partition;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.HashUtils;

/**
 * Partition of tuples by the hash code of the whole tuple.
 */
public final class WholeTupleHashPartitionFunction extends PartitionFunction {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * @param numPartitions total number of partitions.
   */
  @JsonCreator
  public WholeTupleHashPartitionFunction(
      @Nullable @JsonProperty("numPartitions") final Integer numPartitions) {
    super(numPartitions);
  }

  @Override
  public int[] partition(@Nonnull final TupleBatch tb) {
    final int[] result = new int[tb.numTuples()];
    for (int i = 0; i < result.length; i++) {
      int p = HashUtils.hashRow(tb, i) % numPartition();
      if (p < 0) {
        p = p + numPartition();
      }
      result[i] = p;
    }
    return result;
  }
}
