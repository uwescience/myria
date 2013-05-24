package edu.washington.escience.myriad.parallel;

import java.util.Arrays;

import edu.washington.escience.myriad.TupleBatch;

/**
 * Implementation that uses multiple fields as the key to hash
 * 
 * The partition of a tuple is decided by the hash code of a group of fields of the tuple.
 */
public class MultiFieldHashPartitionFunction extends PartitionFunction<String, int[]> {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The field index attribute name. */
  public static final String FIELD_INDEX = "field_index";

  /** The indices used for partitioning. */
  private int[] fieldIndex;

  /**
   * @param numPartition number of partitions
   */
  public MultiFieldHashPartitionFunction(final int numPartition) {
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
      int p = tb.hashCode(i, fieldIndex) % numPartition();
      if (p < 0) {
        p = p + numPartition();
      }
      result[i] = p;
    }
    return result;
  }

  /**
   * @param attribute the attribute name
   * @param value accepts the fields that we want to partition on
   */
  @Override
  public void setAttribute(final String attribute, final int[] value) {
    super.setAttribute(attribute, value);
    if (attribute.equals(FIELD_INDEX)) {
      fieldIndex = Arrays.copyOf(value, value.length);
    }
  }

}