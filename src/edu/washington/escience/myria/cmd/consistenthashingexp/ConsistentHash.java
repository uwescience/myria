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
        int hashVal = node + i * MAGIC_HASHCODE;
        int hashCode = hashDataPoint(hashVal);
        /* In the case of a hash collision, increment the value hashed. */
        while (circle.containsKey(hashCode)) {
          hashVal++;
          hashCode = hashDataPoint(hashVal);
        }
        circle.put(hashCode, new ArrayList<Integer>());
        hashNumbers.get(node).add(hashCode);
      }
    }

    /* Compute the expected skew for this CHT, based on the number of keys assigned to each node. */
    int maxInterval = -1;
    for (Map.Entry<Integer, ArrayList<Integer>> node : hashNumbers.entrySet()) {
      int curInterval = 0;
      /*
       * The number of keys held at a particular bucket is id(bucket) - id(prev bucket). Handle first bucket with null
       * check.
       */
      for (int id : node.getValue()) {
        Integer prev = circle.floorKey(id - 1);
        if (prev == null) {
          prev = circle.lastKey();
        }
        curInterval += id - prev;
      }
      maxInterval = Math.max(maxInterval, curInterval);
    }
    System.out.println("Expected skew = " + (1.0 * maxInterval * hashNumbers.size() / Math.pow(2, 32)));
  }

  /**
   * Add the value to the hash table.
   * 
   * @param value the value to be added
   */
  public void add(final int value) {
    int hashCode = hashDataPoint(value);
    /* We want to stick this data point at the (virtual) node with the "next" key after hashCode. */
    if (circle.lastKey() < hashCode) {
      /* hashCode is after the last (greatest) node key, so it goes in the first entry. */
      circle.firstEntry().getValue().add(value);
    } else {
      /* Some node key is greater than hashCode, so put it there. */
      circle.ceilingEntry(hashCode).getValue().add(value);
    }
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
    return HASH_FUNCTION.newHasher().putInt(val).hash().asInt();
  }
}
