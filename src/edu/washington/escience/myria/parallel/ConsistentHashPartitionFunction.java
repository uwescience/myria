package edu.washington.escience.myria.parallel;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.scaling.ConsistentHash;

/**
 * Implementation that uses one or multiple fields as the key to hash using consistent hashing algorithm.
 * 
 * The partition of a tuple is decided by the hash code of one or a group of fields of the tuple.
 */
public class ConsistentHashPartitionFunction extends PartitionFunction {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The indices used for partitioning. */
  private final int[] fieldIndexes;

  /**
   * @param numPartition number of partitions
   * @param fieldIndexes the indices used for partitioning.
   */
  @JsonCreator
  public ConsistentHashPartitionFunction(@Nullable @JsonProperty("num_partitions") final Integer numPartition,
      @JsonProperty(value = "field_indexes", required = true) final Integer[] fieldIndexes) {
    super(numPartition);
    Preconditions.checkArgument(fieldIndexes.length >= 1, "ConsistentHash requires at least 1 fields to hash");
    this.fieldIndexes = new int[fieldIndexes.length];
    for (int i = 0; i < fieldIndexes.length; ++i) {
      int index = Preconditions.checkNotNull(fieldIndexes[i], "ConsistentHash field index %s cannot be null", i);
      Preconditions.checkArgument(index >= 0, "ConsistentHash field index %s cannot take negative value %s", i, index);
      this.fieldIndexes[i] = index;
    }
  }

  @Override
  public int[] partition(@Nonnull final TupleBatch tb) {
    final int[] result = new int[tb.numTuples()];
    final ConsistentHash consistentHash = ConsistentHash.getInstance();
    for (int i = 0; i < result.length; i++) {
      result[i] = consistentHash.addHashCode(tb.hashCode(i, fieldIndexes));
    }
    return result;
  }
}
