package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.partition.FixValuePartitionFunction;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 *
 * JSON wrapper for BroadcastProducer
 *
 */
public class BroadcastProducerEncoding extends AbstractProducerEncoding<GenericShuffleProducer> {

  @Override
  public GenericShuffleProducer construct(ConstructArgs args) {
    int[][] cellPartition = new int[1][];
    int[] allCells = new int[getRealWorkerIds().size()];
    for (int i = 0; i < getRealWorkerIds().size(); i++) {
      allCells[i] = i;
    }
    cellPartition[0] = allCells;
    return new GenericShuffleProducer(
        null,
        MyriaUtils.getSingleElement(getRealOperatorIds()),
        cellPartition,
        MyriaUtils.integerSetToIntArray(getRealWorkerIds()),
        new FixValuePartitionFunction(0));
  }
}
