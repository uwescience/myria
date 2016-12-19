package edu.washington.escience.myria.operator.network.distribute;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.washington.escience.myria.util.MyriaArrayUtils;

/**
 * Implementation of a DistributeFunction that use the trivial identity hash. (i.e. a --> a) The attribute to hash on
 * must be an INT column and should represent a workerID
 */
public final class IdentityDistributeFunction extends DistributeFunction {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * @param index the column index for distributing
   */
  @JsonCreator
  public IdentityDistributeFunction(@JsonProperty("index") final int index) {
    super(new IdentityPartitionFunction(index), null);
  }

  @Override
  public void setNumDestinations(final int numWorker, final int numOperatorId) {
    partitionFunction.setNumPartitions(numWorker);
    partitionToDestination = MyriaArrayUtils.create2DVerticalIndexList(numWorker);
    numDestinations = numWorker;
  }
}
