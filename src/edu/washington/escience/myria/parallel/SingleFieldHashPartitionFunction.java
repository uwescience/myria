package edu.washington.escience.myria.parallel;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.TupleBatch;

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
  private final int fieldIndex;

  /**
   * @param numPartitions number of partitions.
   * @param fieldIndex the index of the partition field.
   */
  @JsonCreator
  public SingleFieldHashPartitionFunction(@JsonProperty("num_partitions") final Integer numPartitions,
      @JsonProperty(value = "index", required = true) final Integer fieldIndex) {
    super(numPartitions);
    /* TODO(dhalperi) once Jackson actually implements support for required, remove these checks. */
    this.fieldIndex = Objects.requireNonNull(fieldIndex, "missing property index");
    Preconditions.checkArgument(this.fieldIndex >= 0, "SingleFieldHash field index cannot take negative value %s",
        this.fieldIndex);
  }

  /**
   * @param tb data.
   * @return partitions.
   * */
  @Override
  public int[] partition(final TupleBatch tb) {
    final int[] result = new int[tb.numTuples()];
    for (int i = 0; i < result.length; i++) {
      int p = tb.hashCode(i, fieldIndex) % numPartition();
      if (p < 0) {
        p = p + numPartition();
      }
      result[i] = p;
    }
    return result;
  }
}
