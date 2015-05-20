/**
 *
 */
package edu.washington.escience.myria.parallel;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Controls the worker cache, invoked by cache operators.
 */
public final class CacheController {

  /**
   * The logger for this class.
   * */
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerShortMessageProcessor.class);

  /**
   * The worker cache (ideally, a partition with a tuple batch).
   * */
  private final HashMap<Integer, TupleBatch> cache;

  /**
   * The worker cache.
   * */
  private final Worker ownerWorker;

  /**
   * Limitation on the number of tuples that can fit in the Cache.
   * */
  public static final Integer LIMIT_TUPLES = 10000;

  /**
   * Iterator through the HashMap to read by batches.
   * */
  public Iterator it;

  /**
   * Schema of the batch added to the cache.
   */
  private Schema schema;

  /**
   * Constructor for the cache controller.
   * 
   * @param ownerWorker the owner worker
   */
  public CacheController(final Worker ownerWorker) {
    this.ownerWorker = ownerWorker;
    cache = new HashMap<Integer, TupleBatch>();
    it = cache.entrySet().iterator();

    LOGGER.info("Created Cache Controller For " + ownerWorker.getID());
  }

  /**
   * adds tuples to the cache.
   * 
   * @param tb the tuple batch to add
   */
  public void addTupleBatch(final TupleBatch tb) {
    if (tb != null) {
      ownerWorker.LOGGER.info("Inserting batch");
      cache.put(new Random().nextInt(LIMIT_TUPLES), tb);
      // schema now known, so record it (temporary for now, probably not needed)
      schema = tb.getSchema();
    }
  }

  /**
   * Reads from the cache by iterating through the HashMap.
   * 
   * @return the next TupleBatch
   */
  public TupleBatch readTupleBatch() {
    if (it.hasNext()) {
      ownerWorker.LOGGER.info("Fetching batch");
      Map.Entry entry = (Map.Entry) it.next();
      return (TupleBatch) entry.getValue();
    }
    return null;
  }

  /**
   * Reset the iterator after it has read through all the tuple batches.
   */
  public void cacheIteratorReset() {
    ownerWorker.LOGGER.info("RESET Iterator");
    Iterator newIterator = cache.entrySet().iterator();
    ownerWorker.LOGGER.info("entries -- " + cache.size());
    it = newIterator;
  }

  /**
   * Checking if there is anything in the cache to read.
   * 
   * @return a boolean to determine if there is anything to read
   */
  public boolean cacheIteratorHasNext() {
    return it.hasNext();
  }

  /**
   * @return the cache
   */
  public HashMap<Integer, TupleBatch> getCache() {
    return cache;
  }
}
