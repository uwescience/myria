package edu.washington.escience.myria.api.encoding;

import com.google.common.primitives.Ints;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.distribute.BroadcastDistributeFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;

/**
 * JSON wrapper for BroadcastProducer
 */
public class BroadcastProducerEncoding extends AbstractProducerEncoding<GenericShuffleProducer> {

  @Override
  public GenericShuffleProducer construct(ConstructArgs args) {
    GenericShuffleProducer ret =
        new GenericShuffleProducer(
            null,
            getRealOperatorIds().toArray(new ExchangePairID[] {}),
            Ints.toArray(getRealWorkerIds()),
            new BroadcastDistributeFunction());
    ret.getDistributeFunction().setDestinations(getRealWorkerIds().size(), 1);
    return ret;
  }
}
