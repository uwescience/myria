package edu.washington.escience.myria.parallel;

import java.util.Objects;

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
  @JsonProperty
  private final int index;

  /**
   * @param numPartitions number of partitions.
   * @param fieldIndex the index of the partition field.
   */

  public SingleFieldHashPartitionFunction(final Integer numPartitions, final Integer fieldIndex) {
    super(numPartitions);
    /* TODO(dhalperi) once Jackson actually implements support for required, remove these checks. */
    index = Objects.requireNonNull(fieldIndex, "missing property index");
    Preconditions.checkArgument(index >= 0, "SingleFieldHash field index cannot take negative value %s", index);
  }

  /**
   * This is a hot fix, may need to be refined later.
   */
  private SingleFieldHashPartitionFunction() {
    super(0);
    index = 0;
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
