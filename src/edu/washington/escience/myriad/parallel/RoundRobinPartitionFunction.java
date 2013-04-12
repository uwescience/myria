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

  // @Override
  // public final int[] partition(final List<Column<?>> columns, final BitSet validTuples, final Schema schema) {
  // final int[] result = new int[validTuples.cardinality()];
  // int j = 0;
  // for (int i = validTuples.nextSetBit(0); i >= 0; i = validTuples.nextSetBit(i + 1)) {
  // result[j++] = partition;
  // partition = (partition + 1) % numPartition;
  // }
  // return result;
  // // final int numTuples = columns.get(0).size();
  // // final int[] result = new int[numTuples];
  // //
  // // for (int i = 0; i < numTuples; i++) {
  // // result[i] = partition;
  // // partition = (partition + 1) % numPartition;
  // // }
  // // return result;
  // }
  //

  /**
   * @param tb data.
   * @return partitions.
   * */
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