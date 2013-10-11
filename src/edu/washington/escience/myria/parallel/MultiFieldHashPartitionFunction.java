package edu.washington.escience.myria.parallel;

import java.util.Objects;

import edu.washington.escience.myria.TupleBatch;

/**
 * Implementation that uses multiple fields as the key to hash
 * 
 * The partition of a tuple is decided by the hash code of a group of fields of the tuple.
 */
public final class MultiFieldHashPartitionFunction extends PartitionFunction {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The indices used for partitioning. */
  private final int[] fieldIndexes;

  /**
   * @param numPartition number of partitions
   * @param fieldIndexes the indices used for partitioning.
   */
  public MultiFieldHashPartitionFunction(final int numPartition, final int[] fieldIndexes) {
    super(numPartition);
    this.fieldIndexes = Objects.requireNonNull(fieldIndexes);
  }

  @Override
  public int[] partition(final TupleBatch tb) {
    final int[] result = new int[tb.numTuples()];
    for (int i = 0; i < result.length; i++) {
      int p = tb.hashCode(i, fieldIndexes) % numPartition();
      if (p < 0) {
        p = p + numPartition();
      }
      result[i] = p;
    }
    return result;
  }
}