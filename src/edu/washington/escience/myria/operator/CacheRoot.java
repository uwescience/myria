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

/**
 * Consuming tuples from the child and sending them to the CacheController.
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
   * The cache assigned to the worker.
   */
  private CacheController workerCacheController;

  /**
   * @param child the single child of this operator.
   */
  public CacheRoot(final Operator child) {
    super(child);
    workerCacheController = null;
  }

  /**
   * @param child the single child of this operator.
   * @param initChild boolean to determine whether children are also initiated
   */
  public CacheRoot(final Operator child, final boolean initChild) {
    super(child);
    workerCacheController = null;
  }

  @Override
  protected final void consumeTuples(final TupleBatch tuples) throws DbException {
    workerCacheController = getWorker().getCacheController();
    workerCacheController.addTupleBatch(tuples);
  }

  @Override
  protected void childEOS() throws DbException {
  }

  @Override
  protected void childEOI() throws DbException {
  }
}
