package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.network.Consumer;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * Consumer part of JSON encoding for HyperCube Join.
 */
public class HyperCubeShuffleConsumerEncoding extends AbstractConsumerEncoding<Consumer> {

  @Override
  public Consumer construct(ConstructArgs args) {
    return new Consumer(
        null, MyriaUtils.getSingleElement(getRealOperatorIds()), getRealWorkerIds());
  }
}
