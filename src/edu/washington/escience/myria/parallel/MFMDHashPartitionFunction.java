package edu.washington.escience.myria.parallel;

import java.util.Arrays;

import edu.washington.escience.myria.TupleBatch;

/**
 * Multiple field multiple dimension hash partition function for HyperCubeJoin.
 * 
 * @author Shumo Chu <chushumo@cs.washington.edu>
 * 
 */
public class MFMDHashPartitionFunction extends PartitionFunction {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * Partition functions on different dimensions.
   * */
  private final SingleFieldHashPartitionFunction[] partitionFunctions;

  /**
   * 
   * @param numPartition number of buckets
   * @param partitionFunctions define partitions on different dimensions.
   * 
   * */
  public MFMDHashPartitionFunction(final int numPartition, final SingleFieldHashPartitionFunction[] partitionFunctions) {
    super(numPartition);
    this.partitionFunctions = partitionFunctions;
  }

  /**
   * @param tb data.
   * @return partitions.
   * */
  @Override
  public int[] partition(final TupleBatch tb) {
    int[] result = new int[tb.numTuples()];
    Arrays.fill(result, 0);
    for (int i = 0; i < partitionFunctions.length; i++) {
      int[] p = partitionFunctions[i].partition(tb);
      for (int j = 0; j < tb.numTuples(); j++) {
        result[j] = result[j] + p[j];
        if (i != partitionFunctions.length - 1) {
          result[j] = result[j] * partitionFunctions[i].numPartition();
        }
      }
    }

    return result;
  }

}
