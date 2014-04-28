package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.operator.network.Consumer;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.util.MyriaUtils;

public class ConsumerEncoding extends AbstractConsumerEncoding<Consumer> {
  public int[] argWorkerIds;

  @Override
  public Consumer construct(Server server) {
    return new Consumer(null, MyriaUtils.getSingleElement(getRealOperatorIds()), MyriaUtils
        .integerCollectionToIntArray(getRealWorkerIds()));
  }

}