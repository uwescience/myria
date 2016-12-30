package edu.washington.escience.myria.operator.network.distribute;

import com.fasterxml.jackson.annotation.JsonCreator;

import edu.washington.escience.myria.util.MyriaArrayUtils;

/**
 * Round robin distribute function.
 */
public final class RoundRobinDistributeFunction extends DistributeFunction {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   */
  @JsonCreator
  public RoundRobinDistributeFunction() {
    super(new RoundRobinPartitionFunction());
  }

  @Override
  public void setNumDestinations(final int numWorker, final int numOperatorId) {
    partitionFunction.setNumPartitions(numWorker);
    partitionToDestination = MyriaArrayUtils.create2DVerticalIndexList(numWorker);
    numDestinations = numWorker;
  }
}
