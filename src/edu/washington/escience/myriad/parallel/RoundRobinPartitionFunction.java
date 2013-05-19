package edu.washington.escience.myriad.parallel;

import edu.washington.escience.myriad.TupleBatch;

/**
 * A partition function that simply sends one tuple to each output in turn.
 * 
 * @author dhalperi
 * 
 */
public class RoundRobinPartitionFunction extends PartitionFunction<String, Integer> {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The next partition to use. */
  private int partition = 0;

  /**
   * @param numPartitions the number of partitions.
   */
  public RoundRobinPartitionFunction(final int numPartitions) {
    super(numPartitions);
  }

  /**
   * @param tb data.
   * @return partitions.
   */
  @Override
  public final int[] partition(final TupleBatch tb) {
    final int[] result = new int[tb.numTuples()];
    for (int i = 0; i < result.length; i++) {
      result[i] = partition;
      partition = (partition + 1) % numPartition();
    }
    return result;
  }

}