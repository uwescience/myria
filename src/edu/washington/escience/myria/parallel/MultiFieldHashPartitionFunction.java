package edu.washington.escience.myria.parallel;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.TupleBatch;

/**
 * Implementation that uses multiple fields as the key to hash
 * 
 * The partition of a tuple is decided by the hash code of a group of fields of the tuple.
 */
public final class MultiFieldHashPartitionFunction extends PartitionFunction {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The indices used for partitioning. */
  private final int[] fieldIndexes;

  /**
   * @param numPartition number of partitions
   * @param fieldIndexes the indices used for partitioning.
   */
  @JsonCreator
  public MultiFieldHashPartitionFunction(@Nullable @JsonProperty("num_partitions") final Integer numPartition,
      @JsonProperty(value = "field_indexes", required = true) final Integer[] fieldIndexes) {
    super(numPartition);
    Preconditions.checkArgument(fieldIndexes.length > 1, "MultiFieldHash requires at least 2 fields to hash");
    this.fieldIndexes = new int[fieldIndexes.length];
    for (int i = 0; i < fieldIndexes.length; ++i) {
      int index = Preconditions.checkNotNull(fieldIndexes[i], "MultiFieldHash field index %s cannot be null", i);
      Preconditions.checkArgument(index >= 0, "MultiFieldHash field index %s cannot take negative value %s", i, index);
      this.fieldIndexes[i] = index;
    }
  }

  /**
   * @param numPartition number of partitions
   * @param fieldIndexes the indices used for partitioning.
   */
  public MultiFieldHashPartitionFunction(final Integer numPartition, final int[] fieldIndexes) {
    super(numPartition);
    Preconditions.checkArgument(fieldIndexes.length > 1, "MultiFieldHash requires at least 2 fields to hash");
    this.fieldIndexes = fieldIndexes;
    for (int i = 0; i < fieldIndexes.length; ++i) {
      int index = fieldIndexes[i];
      Preconditions.checkArgument(index >= 0, "MultiFieldHash field index %s cannot take negative value %s", i, index);
    }
  }

  @Override
  public int[] partition(@Nonnull final TupleBatch tb) {
    final int[] result = new int[tb.numTuples()];
    for (int i = 0; i < result.length; i++) {
      int p = tb.hashCode(i, fieldIndexes) % numPartition();
      if (p < 0) {
        p = p + numPartition();
      }
      result[i] = p;
    }
    return result;
  }
}