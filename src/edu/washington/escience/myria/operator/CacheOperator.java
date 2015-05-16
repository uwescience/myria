/**
 *
 */
package edu.washington.escience.myria.operator;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.parallel.CacheController;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

/**
 * Consuming from the operator that opened it and sending tuples to the cacheController
 */
public class CacheOperator extends UnaryOperator {

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
  private final CacheController cacheController;

  /**
   * @param child the single child of this operator. In this case, the child it the CC This operator is opened by the
   *          SCC operator
   */
  public CacheOperator(final Operator child) {
    super(child);
    // the operator class was extended to call the getWorker() after the operator called init.
    cacheController = getWorker().getCacheController();
  }

  // this method is going to expect tuples from SCC
  @Override
  protected TupleBatch fetchNextReady() throws Exception {
    // need to post the tuples to the local CC, if limit is reached then drop
    Operator child = getChild();
    // if child has the next set
    for (TupleBatch tb = child.nextReady(); tb != null; tb = child.nextReady()) {
      if (tb.numTuples() <= CacheController.LIMIT_TUPLES - cacheController.getCurrentNumberOfTuples()) {
        // this will deviate and add the batch to the worker cache
        cacheController.addTupleBatch(tb);
      }
      /* Else, don't add it to the cache, but return it anyway for the other operator */
      // ACUTALLY, is this a leaf?
      return tb;
    }
    return null;
  }

  @Override
  protected Schema generateSchema() {
    Operator child = getChild();
    if (child == null) {
      return null;
    }
    return child.getSchema();
  }

}
