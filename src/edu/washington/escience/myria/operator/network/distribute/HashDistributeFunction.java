package edu.washington.escience.myria.operator.network.distribute;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.washington.escience.myria.util.MyriaArrayUtils;

/**
 * Hash distribute function.
 */
public final class HashDistributeFunction extends DistributeFunction {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The indices used for partitioning. */
  @JsonProperty private final int[] indexes;

  /**
   * @param indexes
   */
  @JsonCreator
  public HashDistributeFunction(@JsonProperty("indexes") final int[] indexes) {
    super(new HashPartitionFunction(indexes), null);
    this.indexes = indexes;
  }

  @Override
  public void setNumDestinations(final int numWorker, final int numOperatorId) {
    partitionFunction.setNumPartitions(numWorker);
    partitionToDestination = MyriaArrayUtils.create2DVerticalIndexList(numWorker);
    numDestinations = numWorker;
  }

  /**
   * @return indexes
   */
  public int[] getIndexes() {
    return indexes;
  }
}
