package edu.washington.escience.myria.api.encoding;

import java.util.Map;

import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.CollectConsumer;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.util.MyriaUtils;

public class CollectConsumerEncoding extends AbstractConsumerEncoding<CollectConsumer> {

  @Override
  public void connect(final Operator current, final Map<String, Operator> operators) {
    /* Do nothing. */
  }

  @Override
  public CollectConsumer construct(Server server) {
    return new CollectConsumer(null, MyriaUtils.getSingleElement(getRealOperatorIds()), MyriaUtils
        .integerCollectionToIntArray(getRealWorkerIds()));
  }

}