/**
 *
 */
package edu.washington.escience.myria.operator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.parallel.CacheController;
import edu.washington.escience.myria.parallel.WorkerShortMessageProcessor;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

/**
 * Consuming from the operator that opened it and sending tuples to the cacheController
 */
public class CacheRoot extends RootOperator {

  /**
   * The logger for this class.
   * */
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerShortMessageProcessor.class);

  /**
   **/
  private static final long serialVersionUID = 1L;

  /**
   * Buffer to hold finished and in-progress TupleBatches to send to TBC.
   */
  private TupleBatchBuffer outputBuffer;

  /**
   * The cache assigned to the worker.
   */
  private CacheController cacheController;

  /**
   * @param child the single child of this operator. In this case, the child it the CC This operator is opened by the
   *          SCC operator
   */
  public CacheRoot(final Operator child) {
    super(child);
    cacheController = null;
  }

  @Override
  protected final void consumeTuples(final TupleBatch tuples) throws DbException {

    // the operator class was extended to call the getWorker() after the operator called init.... perhaps
    // it should be initialized elsewhere at the beginning?
    cacheController = getWorker().getCacheController();

    getWorker().LOGGER.info("Worker " + getWorker().getID() + " adding to cache");
    getWorker().LOGGER.info("First CacheController Status " + cacheController.getCurrentNumberOfTuples());

    // need to post the tuples to the local CC, if limit is reached then drop
    Operator child = getChild();

    cacheController.addTupleBatch(tuples);
    getWorker().LOGGER.info("Second CacheController Status " + cacheController.getCurrentNumberOfTuples());

    /*
     * HashMap<Integer, TupleBatch> cacheTemp = cacheController.getCache();
     * 
     * for (Integer tb : cacheTemp.keySet()) { String key = tb.toString(); String value = cacheTemp.get(tb).toString();
     * getWorker().LOGGER.info("HASHMAP: " + key + " " + value); }
     */
  }

  @Override
  protected void childEOS() throws DbException {
  }

  @Override
  protected void childEOI() throws DbException {
  }
}
