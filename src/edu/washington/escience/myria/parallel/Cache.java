/**
 *
 */
package edu.washington.escience.myria.parallel;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Controls the worker cache, invoked by cache operators.
 */
public final class Cache {

  /**
   * The logger for this class.
   * */
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerShortMessageProcessor.class);

  /**
   * The worker cache, where Integer is the sequence number.
   * */
  private final Multimap<Integer, TupleBatch> cache;

  /**
   * Reading Sequence Number -- the cache can read anything below this number
   * */
  private Integer latest_sequence;

  /**
   * The worker cache.
   * */
  private final Worker ownerWorker;

  /**
   * Limitation on the number of tuples that can fit in the Cache.
   * */
  public static final Integer LIMIT_TUPLES = 10000;

  /**
   * Iterator for the TupleBuffers to be read
   */
  private Iterator iterator;

  /**
   * Constructor for the cache controller.
   * 
   * @param ownerWorker the owner worker
   */
  public Cache(final Worker ownerWorker) {
    this.ownerWorker = ownerWorker;
    cache = ArrayListMultimap.create();
    latest_sequence = 0;
    iterator = null;
  }

  /**
   * Adds tuples to the cache using the new sequence (should cycle later).
   * 
   * @param tb the tuple batch to add
   */
  public void addTupleBatch(final TupleBatch tb) {
    if (tb != null) {
      cache.put(latest_sequence, tb);
    }
  }

  /**
   * Reads from the cache by iterating through the MultiMap.
   * 
   * @return the next TupleBatch
   */
  public TupleBatch readTupleBatch() {
    if (iterator.hasNext()) {
      return (TupleBatch) iterator.next();
    }
    return null;
  }

  /**
   * @return boolean whether the cache has more TupleBatches in the collection
   */
  public boolean cacheIteratorHasNext() {
    return iterator.hasNext();
  }

  /**
   * @return the cache
   */
  public Multimap<Integer, TupleBatch> getCache() {
    return cache;
  }

  /**
   * Sets the sequence number to write to.
   * */
  public void setNextSequence() {
    latest_sequence++;
  }

  /**
   * Gets the current latest sequence
   * */
  public Integer getCurrentSequence() {
    return latest_sequence;
  }

  public void readyToRead() {
    /**
     * At this point, we should know which TupleBatches we're reading from
     */
    Collection<TupleBatch> keys_to_read = new HashSet<TupleBatch>();
    for (Integer currentKey : cache.keys()) {
      if (currentKey <= latest_sequence) {
        Collection<TupleBatch> tbCollection = cache.get(currentKey);
        for (TupleBatch tb : tbCollection) {
          keys_to_read.add(tb);
        }
      }
    }
    iterator = keys_to_read.iterator();
  }
}
