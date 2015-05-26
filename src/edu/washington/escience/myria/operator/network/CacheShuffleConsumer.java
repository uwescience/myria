/**
 *
 */
package edu.washington.escience.myria.operator.network;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.parallel.Cache;
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
  }

  @Override
  protected final TupleBatch fetchNextReady() throws DbException {
    Cache workerCacheController = getWorker().getCache();
    try {
      /* adding the tuple batch to the worker cache and returning for parent */
      TupleBatch receivedTb = getTuplesNormal(!nonBlockingExecution);
      workerCacheController.addTupleBatch(receivedTb);
      return receivedTb;
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return null;
  }
}
