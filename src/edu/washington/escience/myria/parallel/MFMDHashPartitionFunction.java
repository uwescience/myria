package edu.washington.escience.myria.parallel;

import java.util.Arrays;

import edu.washington.escience.myria.TupleBatch;

/**
 * Multiple field multiple dimension hash partition function for HyperCubeJoin.
 * 
 * @author Shumo Chu <chushumo@cs.washington.edu>
 * 
 */
public class MFMDHashPartitionFunction extends PartitionFunction<String, SingleFieldHashPartitionFunction[]> {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * The partition functions attribute name.
   * */
  public static final String PARTITON_FUNCTIONS = "partition_functions";

  /**
   * Partition functions on different dimensions.
   * */
  private SingleFieldHashPartitionFunction[] partitionFunctions;

  /**
   * 
   * @param numPartition number of buckets
   * 
   * */
  public MFMDHashPartitionFunction(final int numPartition) {
    super(numPartition);
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

  @Override
  public void setAttribute(final String attribute, final SingleFieldHashPartitionFunction[] value) {
    super.setAttribute(attribute, value);
    if (attribute.equals(PARTITON_FUNCTIONS)) {
      partitionFunctions = value;
    }
  }

}
