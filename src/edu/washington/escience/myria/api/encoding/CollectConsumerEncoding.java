package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.parallel.CollectConsumer;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.util.MyriaUtils;

public class CollectConsumerEncoding extends AbstractConsumerEncoding<CollectConsumer> {

  @Override
  public CollectConsumer construct(Server server) {
    return new CollectConsumer(null, MyriaUtils.getSingleElement(getRealOperatorIds()), MyriaUtils
        .integerCollectionToIntArray(getRealWorkerIds()));
  }

}