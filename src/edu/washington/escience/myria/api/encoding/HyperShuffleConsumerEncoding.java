package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.parallel.GenericShuffleConsumer;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * Consumer part of JSON encoding for HyperCube Join.
 * 
 * @author Shumo Chu <chushumo@cs.washington.edu>
 * 
 */
public class HyperShuffleConsumerEncoding extends AbstractConsumerEncoding<GenericShuffleConsumer> {

  @Override
  public GenericShuffleConsumer construct(Server server) {
    return new GenericShuffleConsumer(null, MyriaUtils.getSingleElement(getRealOperatorIds()), MyriaUtils
        .integerCollectionToIntArray(getRealWorkerIds()));
  }
}
