package edu.washington.escience.myria;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents the partition info of a relation.
 * 
 * @author vaspol
 */
public final class PartitionInfo {

  /** The partition function. */
  private final String partitionFunction;
  /** The hash fields. */
  private final List<Integer> hashFields;

  /**
   * Constructs the partition info object.
   * 
   * @param partitionFunction the partition function
   * @param hashFields the fields that are hashed.
   * */
  public PartitionInfo(final String partitionFunction, final List<Integer> hashFields) {
    if (partitionFunction == null || partitionFunction.equals("")) {
      this.partitionFunction = MyriaConstants.PARTITION_FUNCTION_ROUND_ROBIN;
      this.hashFields = new ArrayList<Integer>();
    } else {
      this.partitionFunction = partitionFunction;
      this.hashFields = hashFields;
    }
  }

  /**
   * @return the partitionFunction
   */
  public String getPartitionFunction() {
    return partitionFunction;
  }

  /**
   * @return the hashFields
   */
  public List<Integer> getHashFields() {
    return hashFields;
  }

  @Override
  public String toString() {
    return "partition function: " + partitionFunction + ", hash indices: " + hashFields;
  }
}
