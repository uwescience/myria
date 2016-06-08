package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.network.GenericShuffleConsumer;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 *
 * JSON wrapper for BroadcastConsumer
 *
 */
public class BroadcastConsumerEncoding extends AbstractConsumerEncoding<GenericShuffleConsumer> {

  @Override
  public GenericShuffleConsumer construct(ConstructArgs args) {
    return new GenericShuffleConsumer(
        null,
        MyriaUtils.getSingleElement(getRealOperatorIds()),
        MyriaUtils.integerSetToIntArray(getRealWorkerIds()));
  }
}
