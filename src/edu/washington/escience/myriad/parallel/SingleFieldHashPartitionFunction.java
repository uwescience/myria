package edu.washington.escience.myriad.parallel;

import java.util.List;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.column.Column;

/**
 * The default implementation of the partition function.
 * 
 * The partition of a tuple is decided by the hash code of a preset field of the tuple.
 */
public class SingleFieldHashPartitionFunction extends PartitionFunction<String, Integer> {

  private static final long serialVersionUID = 1L;

  public static final String FIELD_INDEX = "field_index";

  private Integer fieldIndex;
  public SingleFieldHashPartitionFunction(final int numPartition) {
    super(numPartition);
  }

  @Override
  public int[] partition(final List<Column> columns, final Schema td) {
    final Column partitionColumn = columns.get(this.fieldIndex);
    final int numTuples = partitionColumn.size();
    final int[] result = new int[numTuples];

    for (int i = 0; i < numTuples; i++) {
      int p = partitionColumn.get(i).hashCode() % this.numPartition;
      if (p < 0) {
        p = p + this.numPartition;
      }
      result[i] = p;
    }
    return result;
  }

  /**
   * This partition function only needs the index of the partition field in deciding the tuple partitions
   */
  @Override
  public void setAttribute(final String attribute, final Integer value) {
    super.setAttribute(attribute, value);
    if (attribute.equals(FIELD_INDEX)) {
      this.fieldIndex = value;
    }
  }

}
