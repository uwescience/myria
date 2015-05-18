/**
 *
 */
package edu.washington.escience.myria.operator.network;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.operator.CacheLeaf;
import edu.washington.escience.myria.operator.CacheRoot;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * 
 */
public class CacheShuffleConsumer extends GenericShuffleConsumer {
  /**
   * @param schema input/output data schema
   * @param operatorID my operatorID
   * @param is from which workers the data will come.
   * */
  public CacheShuffleConsumer(final Schema schema, final ExchangePairID operatorID, final int[] is) {
    super(schema, operatorID, is);
    openCacheOperators();
  }

  /**
   * Starting to open the necessary cache operators. (probably re-think this over)
   * */
  public void openCacheOperators() {
    // open a new root and make this it's child... now CSC has two parents
    CacheRoot cacheRootOperator = new CacheRoot(this);
    CacheLeaf cacheLeafOperator = new CacheLeaf();
    try {
      /* problem is that "this" is already open */
      cacheRootOperator.open(getExecEnvVars());
      cacheLeafOperator.open(getExecEnvVars());
    } catch (DbException e) {
      e.printStackTrace();
    }
  }

  /* this is what the CacheShuffleConsumer receives and sends to the parent */
  @Override
  protected final TupleBatch fetchNextReady() throws DbException {
    try {
      /* adding the tuple batch to the worker cache */
      // CacheController workerCacheController = getWorker().getCacheController();
      // workerCacheController.addTupleBatch(getTuplesNormal(!nonBlockingExecution));

      /* here, it would also be receiving tuples from the cache as well */
      /* returning the tuples as usual */
      return getTuplesNormal(!nonBlockingExecution);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return null;
  }
}
