package edu.washington.escience.myria.api.encoding;

import java.util.Map;

import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.Consumer;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.util.MyriaUtils;

public class ConsumerEncoding extends AbstractConsumerEncoding<Consumer> {
  public int[] argWorkerIds;

  @Override
  public Consumer construct(Server server) {
    return new Consumer(null, MyriaUtils.getSingleElement(getRealOperatorIds()), MyriaUtils
        .integerCollectionToIntArray(getRealWorkerIds()));
  }

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    /* Do nothing. */

  }

}