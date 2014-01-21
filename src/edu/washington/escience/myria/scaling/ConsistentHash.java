package edu.washington.escience.myria.scaling;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * Represents Consistent Hashing.
 * 
 * @author vaspol
 */
public class ConsistentHash {

  /** Class-specific magic number used to generate the hash code. */
  private static final int MAGIC_HASHCODE = 243;
  /** The hash function for this class. */
  private static final HashFunction HASH_FUNCTION = Hashing.murmur3_32(MAGIC_HASHCODE);
  /**
   * the circle representation of the data structure. Only contains the mapping between the hash value to the node id.
   * The node id is assumed to be a string by using the worker's address i.e. vega.cs.washington.edu
   */
  private final TreeMap<Integer, Integer> circle;

  /** mapping between the node id to its hash value assumption: node is an integer. */
  private final Map<Integer, Set<ConsistentHashInterval>> hashNumbers;

  /** the number of virtual nodes per one physical node. */
  private final int numReplicas;

  /**
   * Constructs the data structure with consistent hashing with the specified size.
   * 
   * @param numReplicas The number of replicas on the circle.
   */
  public ConsistentHash(final int numReplicas) {
    circle = new TreeMap<Integer, Integer>();
    hashNumbers = new HashMap<Integer, Set<ConsistentHashInterval>>();
    this.numReplicas = numReplicas;
  }

  /**
   * Returns the intervals of the worker.
   * 
   * @param workerId The worker's id
   * @return A set containing all the intervals that belongs to the worker
   */
  public Set<ConsistentHashInterval> getIntervals(final int workerId) {
    return new HashSet<ConsistentHashInterval>(hashNumbers.get(workerId));
  }

  /**
   * Adds a worker to the consistent hashing circle. To get the intervals of the node, the user must call
   * getIntervals().
   * 
   * @param workerId the id of the worker to be added.
   */
  public void addWorker(final int workerId) {
    for (int i = 0; i < numReplicas; i++) {
      int hashVal = workerId + i * MAGIC_HASHCODE;
      int hashCode = hashIntDataPoint(hashVal);
      /* In the case of a hash collision, increment the value hashed. */
      while (circle.containsKey(hashCode)) {
        hashVal++;
        hashCode = hashIntDataPoint(hashVal);
      }
      Integer startInterval = circle.floorKey(hashCode);
      Integer endInterval = circle.ceilingKey(hashCode);

      if (circle.size() == 0) {
        /* The consistent hashing ring is still empty. Add an interval that covers the whole */
        Set<ConsistentHashInterval> interval = new HashSet<ConsistentHashInterval>();
        interval.add(new ConsistentHashInterval(hashCode, hashCode));
        hashNumbers.put(workerId, interval);
        circle.put(hashCode, workerId);
      } else {
        /* The general case where there is at least one virtual node already. */
        if (startInterval == null && endInterval != null) {
          /* Hashed it to the front of the first hash value */
          startInterval = circle.lastKey();
        } else if (startInterval != null && endInterval == null) {
          /* Hashed to the end of the circle */
          endInterval = circle.firstKey();
        }
        circle.put(hashCode, workerId);
        Integer curIntervalWorkerId = circle.get(startInterval);
        Set<ConsistentHashInterval> newWorkerIntervals = hashNumbers.get(workerId);
        Set<ConsistentHashInterval> otherWorkerIntervals = hashNumbers.get(curIntervalWorkerId);
        /* The worker has never been added to the circle. */
        if (newWorkerIntervals == null) {
          newWorkerIntervals = new HashSet<ConsistentHashInterval>();
        }
        otherWorkerIntervals.remove(new ConsistentHashInterval(startInterval, endInterval));
        otherWorkerIntervals.add(new ConsistentHashInterval(startInterval, hashCode));
        newWorkerIntervals.add(new ConsistentHashInterval(hashCode, endInterval));
        hashNumbers.put(workerId, newWorkerIntervals);
      }
    }
  }

  /**
   * Add the value to the hash table.
   * 
   * @param value the value to be added
   * @return the worker id that the data belongs to.
   */
  public int addIntData(final int value) {
    int hashCode = hashIntDataPoint(value);
    /* We want to stick this data point at the (virtual) node with the "next" key after hashCode. */
    if (circle.lastKey() < hashCode) {
      /* hashCode is after the last (greatest) node key, so it goes in the first entry. */
      return circle.firstEntry().getValue();
    } else {
      /* Some node key is greater than hashCode, so put it there. */
      return circle.ceilingEntry(hashCode).getValue();
    }
  }

  /**
   * Hash val.
   * 
   * @param val the value to be hashed
   * @return the hashcode of val
   */
  private int hashIntDataPoint(final int val) {
    return Math.abs(HASH_FUNCTION.newHasher().putInt(val).hash().asInt());
  }
}
