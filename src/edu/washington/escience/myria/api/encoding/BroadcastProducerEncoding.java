package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.parallel.FixValuePartitionFunction;
import edu.washington.escience.myria.parallel.GenericShuffleProducer;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * 
 * JSON wrapper for BroadcastProducer
 * 
 * @author Shumo Chu <chushumo@cs.washington.edu>
 * 
 */
public class BroadcastProducerEncoding extends AbstractProducerEncoding<GenericShuffleProducer> {

  @Override
  public GenericShuffleProducer construct(Server server) {
    int[][] cellPartition = new int[1][];
    int[] allCells = new int[getRealWorkerIds().size()];
    for (int i = 0; i < getRealWorkerIds().size(); i++) {
      allCells[i] = i;
    }
    cellPartition[0] = allCells;
    return new GenericShuffleProducer(null, MyriaUtils.getSingleElement(getRealOperatorIds()), cellPartition,
        MyriaUtils.integerCollectionToIntArray(getRealWorkerIds()), new FixValuePartitionFunction(0));
  }

}
