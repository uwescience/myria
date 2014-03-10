package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.parallel.GenericShuffleConsumer;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * A JSON-able wrapper for the expected wire message for a new dataset.
 * 
 */
public class ShuffleConsumerEncoding extends AbstractConsumerEncoding<GenericShuffleConsumer> {

  @Override
  public GenericShuffleConsumer construct(Server server) {
    return new GenericShuffleConsumer(null, MyriaUtils.getSingleElement(getRealOperatorIds()), MyriaUtils
        .integerCollectionToIntArray(getRealWorkerIds()));
  }
}