package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.parallel.GenericShuffleConsumer;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * 
 * JSON wrapper for BroadcastConsumer
 * 
 * @author Shumo Chu <chushumo@cs.washington.edu>
 * 
 */
public class BroadcastConsumerEncoding extends AbstractConsumerEncoding<GenericShuffleConsumer> {

  @Override
  public GenericShuffleConsumer construct(Server server) {
    return new GenericShuffleConsumer(null, MyriaUtils.getSingleElement(getRealOperatorIds()), MyriaUtils
        .integerCollectionToIntArray(getRealWorkerIds()));
  }

}
