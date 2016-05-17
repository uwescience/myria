package edu.washington.escience.myria.operator.network.partition;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.HashUtils;

/**
 * The default implementation of the partition function.
 *
 * The partition of a tuple is decided by the hash code of a preset field of the tuple.
 */
public final class SingleFieldHashPartitionFunction extends PartitionFunction {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * The index of the partition field.
   */
  @JsonProperty private final int index;

  /**
   * The index of the chosen hashcode in <code>HashUtils</code>.
   */
  @JsonProperty private final int seedIndex;

  /**
   * @param numPartitions number of partitions.
   * @param index the index of the partition field.
   * @param seedIndex the index of chosen hash seed.
   */
  @JsonCreator
  public SingleFieldHashPartitionFunction(
      @JsonProperty("numPartitions") final Integer numPartitions,
      @JsonProperty(value = "index", required = true) final Integer index,
      @JsonProperty(value = "seedIndex") final Integer seedIndex) {
    super(numPartitions);
    /* TODO(dhalperi) once Jackson actually implements support for required, remove these checks. */
    this.index = java.util.Objects.requireNonNull(index, "missing property index");
    this.seedIndex = MoreObjects.firstNonNull(seedIndex, 0) % HashUtils.NUM_OF_HASHFUNCTIONS;
    Preconditions.checkArgument(
        this.index >= 0, "SingleFieldHash field index cannot take negative value %s", this.index);
  }

  /**
   * @param numPartitions numPartitions number of partitions.
   * @param index the index of the partition field.
   */
  public SingleFieldHashPartitionFunction(final Integer numPartitions, final Integer index) {
    this(numPartitions, index, Integer.valueOf(0));
  }

  /**
   * @return the index
   */
  public int getIndex() {
    return index;
  }

  /**
   * @param tb data.
   * @return partitions.
   * */
  @Override
  public int[] partition(final @Nonnull TupleBatch tb) {
    final int[] result = new int[tb.numTuples()];
    for (int i = 0; i < result.length; i++) {
      int p = HashUtils.hashValue(tb, index, i, seedIndex) % numPartition();
      if (p < 0) {
        p = p + numPartition();
      }
      result[i] = p;
    }
    return result;
  }
}
