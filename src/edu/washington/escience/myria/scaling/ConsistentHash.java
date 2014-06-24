package edu.washington.escience.myria.scaling;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.base.Charsets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * Represents Consistent Hashing.
 */
public final class ConsistentHash {

  /** Class-specific magic number used to generate the hash code. */
  private static final int MAGIC_HASHCODE = 243;
  /** The hash function for this class. */
  private static final HashFunction HASH_FUNCTION = Hashing.murmur3_32(MAGIC_HASHCODE);
  /**
   * the circle representation of the data structure. Only contains the mapping between the hash value to the node id.
   * The node id is assumed to be a string by using the worker's address i.e. vega.cs.washington.edu
   */
  private final TreeMap<Integer, Integer> circle;

  /**
   * represents a mapping between the worker id to the partition number because the partition in tuplebatch only
   * supports the partition number not the worker id.
   */
  private final Map<Integer, Integer> partitionNumber;

  /** mapping between the node id to its hash value assumption: node is an integer. */
  private final Map<Integer, Set<ConsistentHashInterval>> hashNumbers;

  /** the number of virtual nodes per one physical node. */
  private final int numReplicas;

  /** the singleton instance of the consistent hashing structure. */
  private static ConsistentHash instance;

  /**
   * Constructs the data structure with consistent hashing with the specified size.
   * 
   * @param numReplicas The number of replicas on the circle.
   */
  private ConsistentHash(final int numReplicas) {
    circle = new TreeMap<Integer, Integer>();
    hashNumbers = new HashMap<Integer, Set<ConsistentHashInterval>>();
    this.numReplicas = numReplicas;
    partitionNumber = new HashMap<Integer, Integer>();
  }

  /**
   * Initializes the consistent hashing structure. Every time initialize is called, the structure will be reset.
   * 
   * @param numReplicas the number of replicas this consistent hashing is using
   */
  public static void initialize(final int numReplicas) {
    instance = new ConsistentHash(numReplicas);
  }

  /**
   * @return the singleton instance of consistent hashing.
   */
  public static ConsistentHash getInstance() {
    return instance;
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
   * This method should only be used when starting up the cluster and populating the intervals from the stored
   * information. It assumes that the number of virtual replica specified from the cluster is equal to the one used when
   * storing the ConsistentHash information.
   * 
   * @param intervals the intervals being populated
   */
  public void addIntervals(final Set<ConsistentHashInterval> intervals) {
    for (ConsistentHashInterval interval : intervals) {
      /* populate all the structure for consistent hash */
      int workerId = interval.getWorkerId();
      int workerHashCode = hashIntDataPoint(workerId);
      circle.put(workerHashCode, workerId);
      if (!hashNumbers.containsKey(workerId)) {
        hashNumbers.put(workerId, new HashSet<ConsistentHashInterval>());
      }
      hashNumbers.get(workerId).add(interval);
      partitionNumber.put(workerId, interval.getPartition());
    }
  }

  /**
   * Adds a worker to the consistent hashing circle. To get the intervals of the node, the user must call
   * getIntervals().
   * 
   * @param workerId the id of the worker to be added.
   */
  public void addWorker(final int workerId) {
    int partition = partitionNumber.size();
    partitionNumber.put(workerId, partition);
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
        interval.add(new ConsistentHashInterval(hashCode, hashCode, workerId, partition));
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
        otherWorkerIntervals.remove(new ConsistentHashInterval(startInterval, endInterval, workerId, partition));
        otherWorkerIntervals.add(new ConsistentHashInterval(startInterval, hashCode, workerId, partition));
        newWorkerIntervals.add(new ConsistentHashInterval(hashCode, endInterval, workerId, partition));
        hashNumbers.put(workerId, newWorkerIntervals);
      }
    }
  }

  /**
   * Returns the partition number for that hashCode.
   * 
   * @param hashCode the hash code to be mapped to the partition number
   * @return the partition number of the hash code
   */
  public int addHashCode(final int hashCode) {
    int workerId;
    /* We want to stick this data point at the (virtual) node with the "next" key after hashCode. */
    if (circle.lastKey() < hashCode) {
      /* hashCode is after the last (greatest) node key, so it goes in the first entry. */
      workerId = circle.firstEntry().getValue();
    } else {
      /* Some node key is greater than hashCode, so put it there. */
      workerId = circle.ceilingEntry(hashCode).getValue();
    }
    return partitionNumber.get(workerId);
  }

  /**
   * Add the value to the hash table.
   * 
   * @param value the value to be added
   * @return the worker id that the data belongs to.
   */
  public int addInt(final int value) {
    int hashCode = hashIntDataPoint(value);
    return addHashCode(hashCode);
  }

  /**
   * Add the value to the hash table.
   * 
   * @param value the value to be added
   * @return the worker id that the data belongs to.
   */
  public int addString(final String value) {
    int hashCode = hashStringDataPoint(value);
    return addHashCode(hashCode);
  }

  /**
   * Hash val.
   * 
   * @param val the value to be hashed
   * @return the hashcode of val
   */
  private int hashIntDataPoint(final int val) {
    return HASH_FUNCTION.newHasher().putInt(val).hash().asInt();
  }

  /**
   * Hash val.
   * 
   * @param val the value to be hashed
   * @return the hashcode of val
   */
  private int hashStringDataPoint(final String val) {
    return HASH_FUNCTION.newHasher().putString(val, Charsets.UTF_8).hash().asInt();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Entry<Integer, Integer> entry : circle.entrySet()) {
      sb.append("hash_value: " + entry.getKey() + ", worker_id: " + entry.getValue());
      sb.append("[");
      int i = 0;
      for (ConsistentHashInterval interval : hashNumbers.get(entry.getValue())) {
        sb.append(interval.toString());
        if (i < hashNumbers.size()) {
          sb.append(", ");
        }
        ++i;
      }
      sb.append("]");
    }
    return sb.toString();
  }
}
