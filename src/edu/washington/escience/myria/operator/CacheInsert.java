/**
 *
 */
package edu.washington.escience.myria.operator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.parallel.Cache;
import edu.washington.escience.myria.parallel.WorkerShortMessageProcessor;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Consuming tuples from the child and sending them to the CacheController.
 */
public class CacheInsert extends RootOperator {

  /**
   * The logger for this class.
   * */
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerShortMessageProcessor.class);

  /**
   **/
  private static final long serialVersionUID = 1L;

  /**
   * The cache assigned to the worker.
   */
  private Cache workerCache;

  /**
   * @param child the single child of this operator.
   */
  public CacheInsert(final Operator child) {
    super(child);
    workerCache = null;
  }

  @Override
  protected final void consumeTuples(final TupleBatch tuples) throws DbException {
    workerCache = getWorker().getCache();
    workerCache.addTupleBatch(tuples);
  }

  @Override
  protected void childEOS() throws DbException {
  }

  @Override
  protected void childEOI() throws DbException {
  }
}
