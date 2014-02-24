package edu.washington.escience.myria.api.encoding;

import java.util.Map;

import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.CollectProducer;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.util.MyriaUtils;

public class CollectProducerEncoding extends AbstractProducerEncoding<CollectProducer> {
  @Required
  public String argChild;

  @Override
  public void connect(final Operator current, final Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild) });
  }

  @Override
  public CollectProducer construct(Server server) {
    return new CollectProducer(null, MyriaUtils.getSingleElement(getRealOperatorIds()), MyriaUtils
        .getSingleElement(getRealWorkerIds()));
  }

}