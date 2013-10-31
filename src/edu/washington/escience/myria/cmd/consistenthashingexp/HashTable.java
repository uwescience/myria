package edu.washington.escience.myria.cmd.consistenthashingexp;

/**
 * Represents a hash table
 */
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * Represents a HashTable.
 * 
 * @author vaspol
 * 
 */
public final class HashTable {

  /** Class-specific magic number used to generate the hash code. */
  private static final int MAGIC_HASHCODE = 243;
  /** The hash function for this class. */
  private static final HashFunction HASH_FUNCTION = Hashing.murmur3_32(MAGIC_HASHCODE);
  /** The default number of buckets. */
  private static final int DEFAULT_BUCKET_SIZE = 10;
  /** The list representing the buckets. */
  private final List<List<Integer>> buckets;

  /**
   * Constructs the hash table with the default bucket size.
   */
  public HashTable() {
    this(DEFAULT_BUCKET_SIZE);
  }

  /**
   * Constructs the hash table with the specified size.
   * 
   * @param initialBucketSize the initial bucket size
   */
  public HashTable(final int initialBucketSize) {
    buckets = new ArrayList<List<Integer>>(initialBucketSize);
    for (int i = 0; i < initialBucketSize; i++) {
      buckets.add(new LinkedList<Integer>());
    }
  }

  /**
   * Add the value to the hash table.
   * 
   * @param value the value to be added
   */
  public void add(final int value) {
    int index = HASH_FUNCTION.newHasher().putInt(value).hash().asInt() % buckets.size();
    List<Integer> bucket = buckets.get(Math.abs(index));
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
}
