/**
 *
 */
package edu.washington.escience.myria.operator.network;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.parallel.CacheController;
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
    try {
      /* adding the tuple batch to the worker cache */
      CacheController workerCacheController = getWorker().getCacheController();
      workerCacheController.addTupleBatch(getTuplesNormal(!nonBlockingExecution));

      /* returning the tuples as usual */
      return getTuplesNormal(!nonBlockingExecution);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return null;
  }
  // This class needs to open the CacheRoot operator and make this operator it's child
}
