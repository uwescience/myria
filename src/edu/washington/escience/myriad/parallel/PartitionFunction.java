package edu.washington.escience.myriad.parallel;

import java.io.Serializable;
import java.util.HashMap;

import edu.washington.escience.myriad.TupleBatch;

/**
 * The ShuffleProducer class uses an instance of the PartitionFunction class to decide which worker a tuple should be
 * routed to. Typically, the ShuffleProducer class invokes {@link partition(Tuple, Schema) partition} on every tuple it
 * generates.
 */
public abstract class PartitionFunction<K, V> implements Serializable {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  HashMap<K, V> attributes = new HashMap<K, V>();
  int numPartition;

  /**
   * Each partition function implementation must has a Class(int) style constructor
   */
  public PartitionFunction(final int numPartition) {
    this.numPartition = numPartition;
  }

  public V getAttribute(final K attribute) {
    return attributes.get(attribute);
  }

  public int numPartition() {
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
  // * Given an input tuple t, determine which partition to route it to.
  // *
  // * Note: Schema schema is explicitly required even though the Tuple t includes a Schema (obtained by calling
  // * t.getSchema()) since field names might be absent from t.getSchema(), and the PartitionFunction might require
  // field
  // * names.
  // * // * @param schema the tuple descriptor of the input tuple. Must have non-null names for those attributes that
  // are
  // * used to compute the worker to route to.
  // * @param validTuples denoting which tuples in columns are valid. Other tuples in columns should not be included in
  // * partition
  public abstract int[] partition(TupleBatch data);

  // public abstract int[] partition(List<Column<?>> columns, BitSet validTuples, Schema schema);

  /**
   * A concrete implementation of a partition function may need some information to help it decide the tuple partitions.
   */
  public void setAttribute(final K attribute, final V value) {
    this.attributes.put(attribute, value);
  }

}
