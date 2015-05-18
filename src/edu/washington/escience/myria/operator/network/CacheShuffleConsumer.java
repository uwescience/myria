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
    // TODO Auto-generated constructor stub
  }

  // NOTE EITHER we push the batch to the cacheController of the worker or we open the cacheRoot operator, making
  // sure that the cacheRoot sees this as a child

  @Override
  protected final TupleBatch fetchNextReady() throws DbException {
    try {

      CacheController localCacheController = getWorker().getCacheController();

      getWorker().LOGGER.info("Worker " + getWorker().getID() + " adding to cache");
      getWorker().LOGGER.info("First CacheController Status " + localCacheController.getCurrentNumberOfTuples());
      // @JORTIZ: this tuple batch is different because it needs to be added to the cacheController
      localCacheController.addTupleBatch(getTuplesNormal(!nonBlockingExecution));

      getWorker().LOGGER.info("Second CacheController Status " + localCacheController.getCurrentNumberOfTuples());
      getWorker().LOGGER.info("Final Size " + localCacheController.getCache().values());

      // finally, return tuples as usual
      return getTuplesNormal(!nonBlockingExecution);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return null;
  }
  // This class needs to open the CacheRoot operator and make this operator it's child
}
