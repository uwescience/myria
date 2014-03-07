package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.parallel.CollectProducer;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.util.MyriaUtils;

public class CollectProducerEncoding extends AbstractProducerEncoding<CollectProducer> {

  @Override
  public CollectProducer construct(Server server) {
    return new CollectProducer(null, MyriaUtils.getSingleElement(getRealOperatorIds()), MyriaUtils
        .getSingleElement(getRealWorkerIds()));
  }

}