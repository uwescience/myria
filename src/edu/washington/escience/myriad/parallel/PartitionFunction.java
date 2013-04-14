package edu.washington.escience.myriad.parallel;

import java.io.Serializable;
import java.util.HashMap;

import edu.washington.escience.myriad.TupleBatch;

/**
 * The ShuffleProducer class uses an instance of the PartitionFunction class to decide which worker a tuple should be
 * routed to. Typically, the ShuffleProducer class invokes {@link partition(Tuple, Schema) partition} on every tuple it
 * generates.
 * 
 * @param <K> the type that attributes of this partition function have. Usually String.
 * @param <V> the value that attributed of this partition function have. Usually Integer.
 */
public abstract class PartitionFunction<K, V> implements Serializable {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * attributes of this partition function.
   * */
  private final HashMap<K, V> attributes = new HashMap<K, V>();

  /**
   * number of partitions.
   * */
  private final int numPartition;

  /**
   * Each partition function implementation must has a Class(int) style constructor.
   * 
   * @param numPartition number of partitions.
   */
  public PartitionFunction(final int numPartition) {
    this.numPartition = numPartition;
  }

  /**
   * @return attribute value.
   * @param attribute attribute key.
   * */
  public final V getAttribute(final K attribute) {
    return attributes.get(attribute);
  }

  /**
   * @return the number of partitions.
   * */
  public final int numPartition() {
    return this.numPartition;
  }

  /**
   * Given that the TupleBatches expose only the valid tuples, partition functions using TB.get** methods should be of
   * little overhead comparing with direct Column access .
   * 
   * @param data data TupleBatch.
   * 
   * @return the partition
   * 
   */
  public abstract int[] partition(TupleBatch data);

  /**
   * A concrete implementation of a partition function may need some information to help it decide the tuple partitions.
   * 
   * @param attribute an attribute.
   * @param value the value of that attribute.
   */
  public void setAttribute(final K attribute, final V value) {
    this.attributes.put(attribute, value);
  }

}
