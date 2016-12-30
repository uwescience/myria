package edu.washington.escience.myria.api.encoding;

import com.google.common.primitives.Ints;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.network.Consumer;
import edu.washington.escience.myria.util.MyriaUtils;

public class ConsumerEncoding extends AbstractConsumerEncoding<Consumer> {
  public int[] argWorkerIds;

  @Override
  public Consumer construct(ConstructArgs args) {
    return new Consumer(
        null, MyriaUtils.getSingleElement(getRealOperatorIds()), Ints.toArray(getRealWorkerIds()));
  }
}
