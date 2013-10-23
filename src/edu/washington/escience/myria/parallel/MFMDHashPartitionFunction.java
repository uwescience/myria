package edu.washington.escience.myria.parallel;

import java.util.Arrays;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.TupleBatch;

/**
 * Multiple field multiple dimension hash partition function for HyperCubeJoin.
 * 
 * @author Shumo Chu <chushumo@cs.washington.edu>
 * 
 */
public final class MFMDHashPartitionFunction extends PartitionFunction {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * Partition functions on different dimensions.
   */
  private final SingleFieldHashPartitionFunction[] partitionFunctions;

  /**
   * 
   * @param numPartitions number of buckets
   * @param hypercubeDimensions the sizes of each dimension of the hypercube.
   * @param fieldIndexes which fields are hashed.
   * 
   */
  public MFMDHashPartitionFunction(final int numPartitions, final int[] hypercubeDimensions, final int[] fieldIndexes) {
    super(numPartitions);
    partitionFunctions = new SingleFieldHashPartitionFunction[fieldIndexes.length];
    for (int i = 0; i < fieldIndexes.length; ++i) {
      Preconditions.checkPositionIndex(fieldIndexes[i], hypercubeDimensions.length);
      partitionFunctions[i] =
          new SingleFieldHashPartitionFunction(hypercubeDimensions[fieldIndexes[i]], fieldIndexes[i]);
    }
  }

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
