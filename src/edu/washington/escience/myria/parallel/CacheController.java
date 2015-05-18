/**
 *
 */
package edu.washington.escience.myria.parallel;

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

/**
 * Controls the worker cache, invoked by cache operators.
 */
public final class CacheController {

  /**
   * The logger for this class.
   * */
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerShortMessageProcessor.class);

  /**
   * The worker cache.
   * */
  private final HashMap<Integer, TupleBatch> cache;

  /**
   * The incoming tuples --- create buffer too?
   * */
  // private final HashMap<Integer, TupleBatch> incomingTuples;

  /**
   * Buffer to hold finished and in-progress TupleBatches to place in cache.
   */
  private TupleBatchBuffer outputBuffer;

  /**
   * * The worker who owns this cache.
   * */
  private final Worker ownerWorker;

  /**
   * @JORTIZ These three variables are temp for now
   */
  public static final Integer LIMIT_TUPLES = 10000;
  private Integer tupleCount;
  private Integer keysUsed;

  /**
   * Constructor for the cache controller.
   * 
   * @param ownerWorker the owner worker
   */
  public CacheController(final Worker ownerWorker) {
    cache = new HashMap<Integer, TupleBatch>();
    // incomingTuples = new HashMap<Integer, TupleBatch>();
    this.ownerWorker = ownerWorker;
    keysUsed = 0;
    tupleCount = 0;
    LOGGER.info("INTIALIZED CACHE CONTROLLER FOR " + ownerWorker.getID());
  }

  /**
   * adds tuples to the cache.
   */
  public void addTupleBatch(final TupleBatch tb) {
    tupleCount += TupleBatch.BATCH_SIZE;
    cache.put(keysUsed++, tb);
    // LOGGER.info("WORKER " + ownerWorker.getID() + " Stored for key " + (keysUsed - 1));
  }

  /**
   * Reads from the cache.
   */
  public void readTupleBatch() {
    // TODO: implement
  }

  /**
   * @return tupleCount the number of tuples currently in the cache
   */
  public Integer getCurrentNumberOfTuples() {
    return tupleCount;
  }

  /**
   * @return tupleCount the number of tuples currently in the cache
   */
  public Integer getKeys() {
    return keysUsed;
  }

  /**
   * @return the number of tuples currently in the cache
   */
  public HashMap<Integer, TupleBatch> getCache() {
    return cache;
  }
}
