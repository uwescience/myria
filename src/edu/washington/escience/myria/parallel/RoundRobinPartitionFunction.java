package edu.washington.escience.myria.parallel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.washington.escience.myria.TupleBatch;

/**
 * A partition function that simply sends one tuple to each output in turn.
 * 
 * @author dhalperi
 * 
 */
public final class RoundRobinPartitionFunction extends PartitionFunction {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The next partition to use. */
  private int partition = 0;

  /**
   * @param numPartitions the number of partitions.
   */
  @JsonCreator
  public RoundRobinPartitionFunction(@JsonProperty("num_partitions") final Integer numPartitions) {
    super(numPartitions);
  }

  /**
   * @param tb data.
   * @return partitions.
   */
  @Override
  public int[] partition(final TupleBatch tb) {
    final int[] result = new int[tb.numTuples()];
    for (int i = 0; i < result.length; i++) {
      result[i] = partition;
      partition = (partition + 1) % numPartition();
    }
    return result;
  }

}