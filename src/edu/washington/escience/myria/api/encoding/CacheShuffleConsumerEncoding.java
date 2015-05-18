/**
 *
 */
package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.network.CacheShuffleConsumer;
import edu.washington.escience.myria.operator.network.GenericShuffleConsumer;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * 
 */
public class CacheShuffleConsumerEncoding extends ShuffleConsumerEncoding {
  @Override
  public GenericShuffleConsumer construct(final ConstructArgs args) {
    // calling the cache shuffle consumer
    return new CacheShuffleConsumer(null, MyriaUtils.getSingleElement(getRealOperatorIds()), MyriaUtils
        .integerSetToIntArray(getRealWorkerIds()));
  }
}
