package edu.washington.escience.myria.operator.network.partition;

import java.util.Arrays;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Multiple field multiple dimension hash partition function for HyperCubeJoin.
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
   * @param hashedColumns which fields are hashed.
   * @param mappedHCDimensions mapped hypercube dimensions of hashed columns.
   *
   */
  public MFMDHashPartitionFunction(
      final int numPartitions,
      final int[] hypercubeDimensions,
      final int[] hashedColumns,
      final int[] mappedHCDimensions) {
    super(numPartitions);
    partitionFunctions = new SingleFieldHashPartitionFunction[hashedColumns.length];
    for (int i = 0; i < hashedColumns.length; ++i) {
      Preconditions.checkPositionIndex(hashedColumns[i], hypercubeDimensions.length);
      Preconditions.checkArgument(
          hashedColumns.length == mappedHCDimensions.length,
          "hashedColumns must have the same arity as mappedHCDimensions");
      partitionFunctions[i] =
          new SingleFieldHashPartitionFunction(
              hypercubeDimensions[mappedHCDimensions[i]], hashedColumns[i], mappedHCDimensions[i]);
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
          result[j] = result[j] * partitionFunctions[i + 1].numPartition();
        }
      }
    }

    return result;
  }
}
