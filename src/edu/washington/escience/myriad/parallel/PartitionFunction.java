package edu.washington.escience.myriad.parallel;

import java.io.Serializable;
import java.util.HashMap;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;

/**
 * The ShuffleProducer class uses an instance of the PartitionFunction class to decide which worker
 * a tuple should be routed to. Typically, the ShuffleProducer class invokes {@link partition(Tuple,
 * Schema) partition} on every tuple it generates.
 * */
public abstract class PartitionFunction<K, V> implements Serializable {

  private static final long serialVersionUID = 1L;

  HashMap<K, V> attributes = new HashMap<K, V>();
  int numPartition;

  /**
   * Each partition function implementation must has a Class(int) style constructor
   * */
  public PartitionFunction(int numPartition) {
    this.numPartition = numPartition;
  }

  public V getAttribute(K attribute) {
    return attributes.get(attribute);
  }

  public int numPartition() {
    return this.numPartition;
  }

  /**
   * Given an input tuple t, determine which partition to route it to.
   * 
   * Note: Schema td is explicitly required even though the Tuple t includes a Schema (obtained by
   * calling t.getSchema()) since field names might be absent from t.getSchema(), and the
   * PartitionFunction might require field names.
   * 
   * 
   * 
   * @param t the input tuple to route.
   * @param td the tuple descriptor of the input tuple. Must have non-null names for those
   *          attributes that are used to compute the worker to route to.
   * 
   * @return the worker to send the tuple to.
   * 
   * */
  public abstract int partition(TupleBatch t, Schema td);

  /**
   * A concrete implementation of a partition function may need some information to help it decide
   * the tuple partitions.
   * */
  public void setAttribute(K attribute, V value) {
    this.attributes.put(attribute, value);
  }

}
