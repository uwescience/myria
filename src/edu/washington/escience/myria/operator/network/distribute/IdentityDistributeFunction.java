package edu.washington.escience.myria.operator.network.distribute;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.washington.escience.myria.util.MyriaArrayUtils;

/**
 * Implementation of a DistributeFunction that maps a tuple to a worker as specified in an INT column (i.e. a --> a).
 */
public final class IdentityDistributeFunction extends DistributeFunction {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * @param index the column index for distributing
   */
  @JsonCreator
  public IdentityDistributeFunction(@JsonProperty("index") final int index) {
    super(new IdentityPartitionFunction(index));
  }

  @Override
  public void setDestinations(int numWorker, int numOperatorId) {
    partitionToDestination = MyriaArrayUtils.create2DVerticalIndexList(numWorker);
    partitionFunction.setNumPartitions(numWorker);
  }
}
