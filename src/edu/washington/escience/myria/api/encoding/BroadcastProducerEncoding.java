package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.distribute.BroadcastDistributeFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * JSON wrapper for BroadcastProducer
 */
public class BroadcastProducerEncoding extends AbstractProducerEncoding<GenericShuffleProducer> {

  @Override
  public GenericShuffleProducer construct(ConstructArgs args) {
    return new GenericShuffleProducer(
        null,
        getRealOperatorIds().toArray(new ExchangePairID[] {}),
        MyriaUtils.integerSetToIntArray(getRealWorkerIds()),
        new BroadcastDistributeFunction(getRealWorkerIds().size()));
  }
}
