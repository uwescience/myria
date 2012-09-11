package edu.washington.escience.myriad.parallel;

import java.util.List;

import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.column.Column;


/**
 * The default implementation of the partition function.
 * 
 * The partition of a tuple is decided by the hash code of a preset field of the tuple.
 * */
public class SingleFieldHashPartitionFunction extends PartitionFunction<String, Integer> {

  private static final long serialVersionUID = 1L;

  public SingleFieldHashPartitionFunction(int numPartition) {
    super(numPartition);
  }

  public static final String FIELD_INDEX = "field_index";
  private Integer fieldIndex;

  /**
   * This partition function only needs the index of the partition field in deciding the tuple
   * partitions
   * */
  @Override
  public void setAttribute(String attribute, Integer value) {
    super.setAttribute(attribute, value);
    if (attribute.equals(FIELD_INDEX))
      this.fieldIndex = value;
  }

  @Override
  public int partition(List<Column> columnData, Type[] columnTypes) {

    // requiredColumns[fieldIndex];
    // int fieldIndex = td.fieldNameToIndex(this.fieldName);
    Column partitionColumn = columnData.get(this.fieldIndex);
    // Field f = t.getField(fieldIndex);
    int remain = columnTypes[fieldIndex].hashCode() % numPartition();
    if (remain < 0)
      remain += numPartition();
    return remain;
  }

}
