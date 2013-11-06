package edu.washington.escience.myria.cmd.consistenthashingexp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * Represents Consistent Hashing, but uses Guava as the hasher.
 * 
 * @author vaspol
 */
public class ConsistentHashingWithGuava {

  /** Class-specific magic number used to generate the hash code. */
  private static final int MAGIC_HASHCODE = 243;
  /** The hash function for this class. */
  private static final HashFunction HASH_FUNCTION = Hashing.murmur3_32(MAGIC_HASHCODE);
  /** the buckets to store the keys. */
  private List<List<Integer>> buckets;
  /** stats of the data movement after new nodes are added to the circle. */
  private final Map<Integer, Integer> dataMoveStats;

  /**
   * Constructs the consistent hashing representation.
   * 
   * @param nodeSize the number of nodes on the circle.
   */
  public ConsistentHashingWithGuava(final int nodeSize) {
    buckets = generateBuckets(nodeSize);
    dataMoveStats = new TreeMap<Integer, Integer>();
  }

  /**
   * @param bucketSize the bucket size
   * @return the buckets with each one of them initialized
   */
  private List<List<Integer>> generateBuckets(final int bucketSize) {
    List<List<Integer>> retval = new ArrayList<List<Integer>>(bucketSize);
    for (int i = 0; i < bucketSize; i++) {
      retval.add(new ArrayList<Integer>());
    }
    return retval;
  }

  /**
   * Add the value to the hash table.
   * 
   * @param value the value to be added
   */
  public void add(final int value) {
    int index = hashDataPoint(value, buckets.size());
    List<Integer> bucket = buckets.get(index);
    bucket.add(value);
  }

  /**
   * Add node(s) to the circle and re-partition the data as necessary.
   * 
   * @param numNodes the number of nodes to resize to
   */
  public void addNode(final int numNodes) {
    List<List<Integer>> newBuckets = generateBuckets(numNodes);
    dataMoveStats.clear();

    int dataMoved = 0;
    // recompute the hash code of each of the elements
    for (int bucketIndex = 0; bucketIndex < buckets.size(); bucketIndex++) {
      List<Integer> bucket = buckets.get(bucketIndex);
      for (Integer value : bucket) {
        int newDestination = hashDataPoint(value, numNodes);
        if (bucketIndex != newDestination) {
          // the data has to move, add a counter to the move stats map
          if (dataMoveStats.containsKey(bucketIndex)) {
            dataMoveStats.put(bucketIndex, dataMoveStats.get(bucketIndex) + 1);
          } else {
            dataMoveStats.put(bucketIndex, 1);
          }
          dataMoved++;
        }
        // add the value to its belonging bucket
        newBuckets.get(newDestination).add(value);
      }
    }
    System.out.println("Total data moved = " + dataMoved);
    buckets = newBuckets;
  }

  /**
   * @return the data move statistics
   */
  public Map<Integer, Integer> getDataMoveStats() {
    return new TreeMap<Integer, Integer>(dataMoveStats);
  }

  /**
   * @return a list containing the count of objects in each bucket
   */
  public List<Integer> getDistribution() {
    List<Integer> result = new ArrayList<Integer>(buckets.size());
    for (List<Integer> bucket : buckets) {
      result.add(bucket.size());
    }
    return result;
  }

  /**
   * @return The skewness of the data that resides in the data structure
   */
  public double getSkewness() {
    int max = Integer.MIN_VALUE;
    long counter = 0;
    for (List<Integer> bucket : buckets) {
      max = Math.max(max, bucket.size());
      counter += bucket.size();
    }
    return (1.0 * max / counter) * buckets.size();
  }

  /**
   * Hash val.
   * 
   * @param val the value to be hashed
   * @param numBuckets the number of buckets in the scheme
   * @return the hashcode of val
   */
  private int hashDataPoint(final int val, final int numBuckets) {
    HashCode hashCode = HASH_FUNCTION.newHasher().putInt(val).hash();
    return Hashing.consistentHash(hashCode, numBuckets);
  }
}
