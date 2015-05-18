/**
 *
 */
package edu.washington.escience.myria.operator;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.parallel.CacheController;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * 
 */
public class CacheLeaf extends LeafOperator {

  /** The schema for the relation stored in the cache. */
  private final Schema schema;

  /**
   * The constructor for the cache leaf operator.
   * */
  public CacheLeaf() {
    // problem, the leaf requires a schema before it is opened/initialized
    // can't check the cacheController at each worker since this operator does not yet know which worker it will be
    // running in
    // work-around: create schema manually for now

    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE), ImmutableList.of("col1", "col2"));
    this.schema = schema;

  }

  @Override
  protected TupleBatch fetchNextReady() throws Exception {
    CacheController workerCacheController = getWorker().getCacheController();

    /* will check if there is anything in the cache before returning a batch */
    getWorker().LOGGER.info("from leaf: " + workerCacheController.cacheIteratorHasNext());
    if (workerCacheController.cacheIteratorHasNext()) {
      return workerCacheController.readTupleBatch();
    }
    return null;
  }

  @Override
  protected Schema generateSchema() {
    /* assumption for now is that all the tuples in the cache have the same schema */
    return schema;
  }

}
