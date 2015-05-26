/**
 *
 */
package edu.washington.escience.myria.operator;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.parallel.Cache;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * 
 */
public class CacheScan extends LeafOperator {

  /** The schema for the relation stored in the cache. */
  private final Schema outputSchema;

  /**
   * The worker cache
   */
  private Cache workerCache;

  /**
   * The constructor for the cache leaf operator.
   * */
  public CacheScan(final Schema schema) {
    outputSchema = schema;
  }

  @Override
  protected TupleBatch fetchNextReady() throws Exception {
    workerCache = getWorker().getCache();
    getWorker().LOGGER.info("Iterator HasNext " + (workerCache.cacheIteratorHasNext()));
    if (workerCache.cacheIteratorHasNext()) {
      return workerCache.readTupleBatch();
    }
    return null;
  }

  @Override
  protected Schema generateSchema() {
    /* assumption for now is that all the tuples in the cache have the same schema */
    return outputSchema;
  }

}
