package edu.washington.escience.myria.parallel;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.storage.TupleBatch;

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
  @JsonProperty
  private final int index;

  /**
   * @param numPartitions number of partitions.
   * @param index the index of the partition field.
   */
  @JsonCreator
  public SingleFieldHashPartitionFunction(@JsonProperty("numPartitions") final Integer numPartitions,
      @JsonProperty(value = "index", required = true) final Integer index) {
    super(numPartitions);
    /* TODO(dhalperi) once Jackson actually implements support for required, remove these checks. */
    this.index = Objects.requireNonNull(index, "missing property index");
    Preconditions.checkArgument(this.index >= 0, "SingleFieldHash field index cannot take negative value %s",
        this.index);
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
  public int[] partition(final TupleBatch tb) {
    final int[] result = new int[tb.numTuples()];
    for (int i = 0; i < result.length; i++) {
      int p = tb.hashCode(i, index) % numPartition();
      if (p < 0) {
        p = p + numPartition();
      }
      result[i] = p;
    }
    return result;
  }
}
