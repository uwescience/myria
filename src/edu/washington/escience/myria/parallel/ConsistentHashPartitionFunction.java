package edu.washington.escience.myria.parallel;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Implementation that uses consistent hashing to hash
 * 
 * The partition of a tuple is decided by the hash code of a group of fields of the tuple and the interval it belongs to
 * in the consistent hash implementation.
 */
public class ConsistentHashPartitionFunction extends PartitionFunction {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The next partition to use. */
  private int partition = 0;
  /** The indices used for partitioning. */
  private final int[] indexes;
  /** The consistent hash object for getting the worker id for the tuple. */
  private final ConsistentHash consistentHash;

  /**
   * @param numPartition number of partitions.
   * @param indexes the indices used for partitioning.
   * @param consistentHash the consistent hash object for getting the worker id for the tuple.
   */
  public ConsistentHashPartitionFunction(final Integer numPartition, final Integer[] indexes,
      final ConsistentHash consistentHash) {
    super(numPartition);
    Preconditions.checkArgument(indexes.length >= 1, "MultiFieldHash requires at least 1 fields to hash");
    this.indexes = new int[indexes.length];
    for (int i = 0; i < indexes.length; ++i) {
      int index = indexes[i];
      Preconditions.checkArgument(index >= 0, "MultiFieldHash field index %s cannot take negative value %s", i, index);
    }
    Preconditions.checkNotNull(consistentHash);
    this.consistentHash = consistentHash;
  }

  @Override
  public int[] partition(@Nonnull final TupleBatch data) {
    final int[] result = new int[data.numTuples()];
    for (int i = 0; i < result.length; i++) {
      int tupleWorker = consistentHash.getWorkerId(data.hashCode(i, indexes));
      if (tupleWorker >= numPartition()) {
        tupleWorker = partition;
        partition = (partition + 1) % numPartition();
      }
      result[i] = tupleWorker;
    }
    return result;
  }

}
