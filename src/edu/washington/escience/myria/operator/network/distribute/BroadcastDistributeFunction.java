package edu.washington.escience.myria.operator.network.distribute;

import com.fasterxml.jackson.annotation.JsonCreator;

import edu.washington.escience.myria.util.MyriaArrayUtils;

/**
 * Broadcast distribute function.
 */
public final class BroadcastDistributeFunction extends DistributeFunction {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   */
  @JsonCreator
  public BroadcastDistributeFunction() {
    super(new SinglePartitionFunction());
  }

  @Override
  public void setDestinations(final int numWorker, final int numOperatorId) {
    partitionToDestination = MyriaArrayUtils.create2DHorizontalIndexList(numWorker * numOperatorId);
  }
}
