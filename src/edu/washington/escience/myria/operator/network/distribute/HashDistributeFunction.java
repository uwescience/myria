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
    super(new HashPartitionFunction(indexes));
    this.indexes = indexes;
  }

  @Override
  public void setDestinations(final int numWorker, final int numOperatorId) {
    partitionToDestination = MyriaArrayUtils.create2DVerticalIndexList(numWorker);
    partitionFunction.setNumPartitions(numWorker);
  }

  /**
   * @return indexes
   */
  public int[] getIndexes() {
    return indexes;
  }
}
