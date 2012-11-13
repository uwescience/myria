package edu.washington.escience.myriad.parallel;

import java.util.List;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.column.Column;

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

  @Override
  public final int[] partition(final List<Column> columns, final Schema td) {
    final int numTuples = columns.get(0).size();
    final int[] result = new int[numTuples];

    for (int i = 0; i < numTuples; i++) {
      result[i] = partition;
      partition = (partition + 1) % numPartition;
    }
    return result;
  }
}