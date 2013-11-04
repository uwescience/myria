package edu.washington.escience.myria.cmd.consistenthashingexp;

import java.util.ArrayList;
import java.util.List;

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
  private final List<List<Integer>> buckets;

  /**
   * Constructs the consistent hashing representation.
   * 
   * @param nodeSize the number of nodes on the circle.
   */
  public ConsistentHashingWithGuava(final int nodeSize) {
    buckets = new ArrayList<List<Integer>>(nodeSize);
    for (int i = 0; i < nodeSize; i++) {
      buckets.add(new ArrayList<Integer>());
    }
  }

  /**
   * Add the value to the hash table.
   * 
   * @param value the value to be added
   */
  public void add(final int value) {
    int index = hashDataPoint(value);
    List<Integer> bucket = buckets.get(index);
    bucket.add(value);
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
   * @return the hashcode of val
   */
  private int hashDataPoint(final int val) {
    HashCode hashCode = HASH_FUNCTION.newHasher().putInt(val).hash();
    return Hashing.consistentHash(hashCode, buckets.size());
  }
}
