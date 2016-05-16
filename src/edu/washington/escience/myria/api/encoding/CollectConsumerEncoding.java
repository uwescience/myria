package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.network.CollectConsumer;
import edu.washington.escience.myria.util.MyriaUtils;

public class CollectConsumerEncoding extends AbstractConsumerEncoding<CollectConsumer> {

  @Override
  public CollectConsumer construct(ConstructArgs args) {
    return new CollectConsumer(
        null,
        MyriaUtils.getSingleElement(getRealOperatorIds()),
        MyriaUtils.integerSetToIntArray(getRealWorkerIds()));
  }
}
