package edu.washington.escience.myriad.parallel;

import edu.washington.escience.myriad.TupleBatch;

/**
 * Partition of tuples by the hash code of the whole tuple.
 */
public final class WholeTupleHashPartitionFunction extends PartitionFunction<String, Integer> {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * @param numPartition total number of partitions.
   * */
  public WholeTupleHashPartitionFunction(final int numPartition) {
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
      int p = tb.hashCode(i) % numPartition();
      if (p < 0) {
        p = p + numPartition();
      }
      result[i] = p;
    }
    return result;
  }

}
