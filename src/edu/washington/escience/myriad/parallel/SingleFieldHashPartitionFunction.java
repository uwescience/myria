package edu.washington.escience.myriad.parallel;

import edu.washington.escience.myriad.TupleBatch;

/**
 * The default implementation of the partition function.
 * 
 * The partition of a tuple is decided by the hash code of a preset field of the tuple.
 */
public final class SingleFieldHashPartitionFunction extends PartitionFunction<String, Integer> {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  public static final String FIELD_INDEX = "field_index";

  private int[] fieldIndex;

  public SingleFieldHashPartitionFunction(final int numPartition) {
    super(numPartition);
  }

  /**
   * @param tb data.
   * @return partitions.
   * */
  @Override
  public int[] partition(final TupleBatch tb) {
    final int[] result = new int[tb.numTuples()];
    for (int i = 0; i < result.length; i++) {
      int p = tb.hashCode(i, fieldIndex) % numPartition;
      if (p < 0) {
        p = p + numPartition;
      }
      result[i] = p;
    }
    return result;
  }

  @Override
  public void setAttribute(final String attribute, final Integer value) {
    super.setAttribute(attribute, value);
    if (attribute.equals(FIELD_INDEX)) {
      fieldIndex = new int[1];
      fieldIndex[0] = value;
    }
  }

}
