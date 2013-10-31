package edu.washington.escience.myria.cmd.consistenthashingexp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
  /** The mod constant. */
  private static final int MOD_CONSTANT = Integer.MAX_VALUE;
  /** the circle representation of the data structure. */
  private final TreeMap<Integer, ArrayList<Integer>> circle;

  /**
   * mapping between the node id to its hash value assumption: node is an integer.
   */
  private final TreeMap<Integer, ArrayList<Integer>> hashNumbers;

  /**
   * Constructs the data structure with consistent hashing with the specified size.
   * 
   * @param nodeId a list of node id
   */
  public ConsistentHash(final ArrayList<Integer> nodeId) {
    this(nodeId, 1);
  }

  /**
   * Constructs a consistent hashing with the specified number of node replicas.
   * 
   * @param nodeIds the node names
   * @param numReplicas the number replicas of each node
   */
  public ConsistentHash(final List<Integer> nodeIds, final int numReplicas) {
    circle = new TreeMap<Integer, ArrayList<Integer>>();
    hashNumbers = new TreeMap<Integer, ArrayList<Integer>>();
    for (Integer node : nodeIds) {
      hashNumbers.put(node, new ArrayList<Integer>());
      for (int i = 0; i < numReplicas; i++) {
        int hashCode = hashDataPoint(node + i);
        int counter = 1;
        while (circle.containsKey(hashCode)) {
          hashCode = hashDataPoint(node + i * MAGIC_HASHCODE + counter);
          counter++;
        }
        circle.put(hashCode, new ArrayList<Integer>());
        hashNumbers.get(node).add(hashCode);
      }
    }
  }

  /**
   * Add the value to the hash table.
   * 
   * @param value the value to be added
   */
  public void add(final int value) {
    int hashCode = hashDataPoint(value);
    int index = -1;
    // TODO: Binary search
    for (Integer val : circle.keySet()) {
      if (hashCode >= val) {
        index = val;
      } else {
        break;
      }
    }
    if (index == -1) {
      for (Integer val : circle.keySet()) {
        index = Math.max(val, index);
      }
    }
    List<Integer> bucket = circle.get(index);
    bucket.add(value);
  }

  /**
   * @return a list containing the count of objects in each bucket
   */
  public Map<Integer, Integer> getDistribution() {
    Map<Integer, Integer> result = new TreeMap<Integer, Integer>();
    for (Integer nodeId : hashNumbers.keySet()) {
      List<Integer> hashesForNode = hashNumbers.get(nodeId);
      int sum = 0;
      for (Integer hashCode : hashesForNode) {
        sum += circle.get(hashCode).size();
      }
      result.put(nodeId, sum);
    }
    return result;
  }

  /**
   * @return The skewness of the data that resides in the data structure
   */
  public double getSkewness() {
    int max = Integer.MIN_VALUE;
    long counter = 0;
    for (Integer nodeId : hashNumbers.keySet()) {
      List<Integer> hashesForNode = hashNumbers.get(nodeId);
      int sum = 0;
      for (Integer hashCode : hashesForNode) {
        int numChildren = circle.get(hashCode).size();
        counter += numChildren;
        sum += numChildren;
      }
      max = Math.max(max, sum);
    }
    return (1.0 * max / counter) * hashNumbers.size();
  }

  /**
   * @return The hash number of all the replicas each node
   */
  public Map<Integer, List<Integer>> getNodesAndReplicas() {
    return new TreeMap<Integer, List<Integer>>(hashNumbers);
  }

  /**
   * Hash val.
   * 
   * @param val the value to be hashed
   * @return the hashcode of val
   */
  private int hashDataPoint(final int val) {
    return Math.abs(HASH_FUNCTION.newHasher().putInt(val).hash().asInt() % MOD_CONSTANT);
  }
}
