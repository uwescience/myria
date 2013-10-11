package edu.washington.escience.myria.parallel;

import java.util.Objects;

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
   * The field index attribute name.
   * */
  public static final String FIELD_INDEX = "field_index";

  /**
   * The index of the partition field.
   */
  private final int fieldIndex;

  /**
   * @param numPartition number of partitions.
   * @param fieldIndex the index of the partition field.
   * */
  public SingleFieldHashPartitionFunction(final int numPartition, final Integer fieldIndex) {
    super(numPartition);
    this.fieldIndex = Objects.requireNonNull(fieldIndex);
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
