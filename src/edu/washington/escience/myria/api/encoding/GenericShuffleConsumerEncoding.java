package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.network.Consumer;
import edu.washington.escience.myria.util.MyriaUtils;

/** JSON wrapper for GenericShuffleConsumer. */
public class GenericShuffleConsumerEncoding extends AbstractConsumerEncoding<Consumer> {

  @Override
  public Consumer construct(ConstructArgs args) {
    return new Consumer(
        null, MyriaUtils.getSingleElement(getRealOperatorIds()), getRealWorkerIds());
  }
}
